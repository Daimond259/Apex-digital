"""Storage layer for transcript persistence (local filesystem or S3)."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class TranscriptStorage:
    """Handles transcript storage to local filesystem or S3."""

    def __init__(self) -> None:
        self.storage_type = os.getenv("TRANSCRIPT_STORAGE_TYPE", "local").lower()
        self.local_path = Path(os.getenv("TRANSCRIPT_LOCAL_PATH", "transcripts"))
        
        self.s3_bucket: Optional[str] = os.getenv("S3_BUCKET")
        self.s3_region: Optional[str] = os.getenv("S3_REGION", "us-east-1")
        self.s3_access_key: Optional[str] = os.getenv("S3_ACCESS_KEY")
        self.s3_secret_key: Optional[str] = os.getenv("S3_SECRET_KEY")
        
        self._s3_client = None
        self._initialized = False

    def _initialize_local(self) -> None:
        """Initialize local storage directory."""
        if not self.local_path.exists():
            self.local_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created transcript storage directory: {self.local_path}")

    def _initialize_s3(self) -> None:
        """Initialize S3 client."""
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            if not all([self.s3_bucket, self.s3_access_key, self.s3_secret_key]):
                raise RuntimeError(
                    "S3 storage requires S3_BUCKET, S3_ACCESS_KEY, and S3_SECRET_KEY environment variables"
                )
            
            self._s3_client = boto3.client(
                's3',
                region_name=self.s3_region,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
            )
            
            self._s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"Initialized S3 storage: bucket={self.s3_bucket}, region={self.s3_region}")
            
        except ImportError:
            raise RuntimeError(
                "boto3 is required for S3 storage. Install with: pip install boto3"
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to initialize S3 storage: {e}")

    def initialize(self) -> None:
        """Initialize storage backend based on configuration."""
        if self._initialized:
            return

        if self.storage_type == "s3":
            self._initialize_s3()
        else:
            if self.storage_type != "local":
                logger.warning(
                    f"Unknown storage type '{self.storage_type}', falling back to local"
                )
                self.storage_type = "local"
            self._initialize_local()
        
        self._initialized = True
        logger.info(f"Transcript storage initialized: {self.storage_type}")

    async def save_transcript(
        self,
        ticket_id: int,
        channel_name: str,
        content: str,
    ) -> Tuple[str, int]:
        """
        Save transcript to storage.
        
        Returns:
            Tuple of (storage_path, file_size_bytes)
        """
        if not self._initialized:
            self.initialize()

        filename = f"ticket-{ticket_id}-{channel_name}.html"
        content_bytes = content.encode("utf-8")
        file_size = len(content_bytes)

        if self.storage_type == "s3":
            return await self._save_to_s3(filename, content_bytes, file_size)
        else:
            return await self._save_to_local(filename, content_bytes, file_size)

    async def _save_to_local(
        self,
        filename: str,
        content_bytes: bytes,
        file_size: int,
    ) -> Tuple[str, int]:
        """Save transcript to local filesystem."""
        file_path = self.local_path / filename
        
        try:
            file_path.write_bytes(content_bytes)
            logger.info(f"Saved transcript to local storage: {file_path}")
            return str(file_path), file_size
        except Exception as e:
            logger.error(f"Failed to save transcript to local storage: {e}")
            raise

    async def _save_to_s3(
        self,
        filename: str,
        content_bytes: bytes,
        file_size: int,
    ) -> Tuple[str, int]:
        """Save transcript to S3."""
        try:
            from botocore.exceptions import ClientError
            import asyncio
            
            s3_key = f"transcripts/{filename}"
            
            def _upload():
                self._s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=content_bytes,
                    ContentType="text/html",
                )
            
            await asyncio.to_thread(_upload)
            logger.info(f"Saved transcript to S3: s3://{self.s3_bucket}/{s3_key}")
            return s3_key, file_size
            
        except ClientError as e:
            logger.error(f"Failed to save transcript to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error saving to S3: {e}")
            raise

    async def retrieve_transcript(self, storage_path: str, storage_type: str) -> Optional[bytes]:
        """
        Retrieve transcript from storage.
        
        Args:
            storage_path: Path or key where the transcript is stored
            storage_type: 'local' or 's3'
            
        Returns:
            Transcript content as bytes, or None if not found
        """
        if storage_type == "s3":
            return await self._retrieve_from_s3(storage_path)
        else:
            return await self._retrieve_from_local(storage_path)

    async def _retrieve_from_local(self, file_path: str) -> Optional[bytes]:
        """Retrieve transcript from local filesystem."""
        try:
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"Transcript not found at local path: {file_path}")
                return None
            
            return path.read_bytes()
        except Exception as e:
            logger.error(f"Failed to retrieve transcript from local storage: {e}")
            return None

    async def _retrieve_from_s3(self, s3_key: str) -> Optional[bytes]:
        """Retrieve transcript from S3."""
        try:
            from botocore.exceptions import ClientError
            import asyncio
            
            if not self._initialized:
                self.initialize()
            
            def _download():
                response = self._s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                return response['Body'].read()
            
            return await asyncio.to_thread(_download)
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Transcript not found in S3: {s3_key}")
            else:
                logger.error(f"Failed to retrieve transcript from S3: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving from S3: {e}")
            return None

    def get_s3_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for S3 object (useful for sharing).
        
        Args:
            s3_key: S3 key of the object
            expiration: URL expiration time in seconds (default: 1 hour)
            
        Returns:
            Presigned URL or None if failed
        """
        try:
            if not self._initialized:
                self.initialize()
            
            if self.storage_type != "s3":
                return None
            
            url = self._s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.s3_bucket, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None
