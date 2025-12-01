"""Async SQLite data layer for Apex Core."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)


class Database:
    """Async database handler using SQLite."""

    def __init__(self, db_path: str | Path = "apex_core.db") -> None:
        self.db_path = Path(db_path)
        self._connection: Optional[aiosqlite.Connection] = None
        self._wallet_lock = asyncio.Lock()
        self.target_schema_version = 7

    async def connect(self) -> None:
        if self._connection is None:
            self._connection = await aiosqlite.connect(self.db_path)
            self._connection.row_factory = aiosqlite.Row
            await self._connection.execute("PRAGMA foreign_keys = ON;")
            await self._connection.commit()
            await self._initialize_schema()

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def _initialize_schema(self) -> None:
        """Initialize the database schema with versioning support."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        # Create the schema_migrations table if it doesn't exist
        await self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await self._connection.commit()

        # Get current schema version
        current_version = await self._get_current_schema_version()
        logger.info(f"Current database schema version: {current_version}")

        # Apply all pending migrations
        await self._apply_pending_migrations(current_version)

        # Log final version
        final_version = await self._get_current_schema_version()
        logger.info(f"Database schema migration complete. Final version: {final_version}")

    async def _get_current_schema_version(self) -> int:
        """Get the current schema version from the migrations table."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT MAX(version) as version FROM schema_migrations"
        )
        row = await cursor.fetchone()
        return row["version"] if row and row["version"] else 0

    async def _apply_pending_migrations(self, current_version: int) -> None:
        """Apply all pending migrations after the current version."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        # Define all migrations in order
        migrations = {
            1: ("base_schema", self._migration_v1),
            2: ("migrate_products_table", self._migration_v2),
            3: ("migrate_discounts_indexes", self._migration_v3),
            4: ("extend_tickets_schema", self._migration_v4),
            5: ("wallet_transactions_table", self._migration_v5),
            6: ("extend_orders_schema", self._migration_v6),
            7: ("transcripts_table", self._migration_v7),
        }

        for version in sorted(migrations.keys()):
            if version > current_version:
                name, migration_fn = migrations[version]
                logger.info(f"Applying migration v{version}: {name}")
                try:
                    await migration_fn()
                    await self._record_migration(version, name)
                    logger.info(f"Migration v{version} applied successfully")
                except Exception as e:
                    logger.error(f"Failed to apply migration v{version}: {e}")
                    raise

    async def _record_migration(self, version: int, name: str) -> None:
        """Record a migration as applied in the schema_migrations table."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
            (version, name),
        )
        await self._connection.commit()

    async def _migration_v1(self) -> None:
        """Migration v1: Create base schema with all core tables."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id INTEGER UNIQUE NOT NULL,
                wallet_balance_cents INTEGER NOT NULL DEFAULT 0,
                total_lifetime_spent_cents INTEGER NOT NULL DEFAULT 0,
                has_client_role INTEGER NOT NULL DEFAULT 0,
                manually_assigned_roles TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                main_category TEXT NOT NULL,
                sub_category TEXT NOT NULL,
                service_name TEXT NOT NULL,
                variant_name TEXT NOT NULL,
                price_cents INTEGER NOT NULL,
                start_time TEXT,
                duration TEXT,
                refill_period TEXT,
                additional_info TEXT,
                role_id INTEGER,
                content_payload TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS discounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product_id INTEGER,
                vip_tier TEXT,
                discount_percent REAL NOT NULL,
                description TEXT,
                expires_at TIMESTAMP,
                is_stackable INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_discord_id INTEGER NOT NULL,
                channel_id INTEGER UNIQUE NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                last_activity TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_discord_id) REFERENCES users(discord_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_discord_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                price_paid_cents INTEGER NOT NULL,
                discount_applied_percent REAL NOT NULL DEFAULT 0,
                order_metadata TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_discord_id) REFERENCES users(discord_id) ON DELETE CASCADE,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            );
            """
        )
        await self._connection.commit()

    async def _migration_v2(self) -> None:
        """Migration v2: Migrate products table from old schema to new schema."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        # Check if the products table has the old 'name' column
        cursor = await self._connection.execute("PRAGMA table_info(products)")
        columns = [row[1] for row in await cursor.fetchall()]

        # If the table has the old 'name' column but not the new columns, migrate it
        if "name" in columns and "main_category" not in columns:
            logger.info("Migrating products table from old schema...")

            # Create a backup of existing products
            await self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS products_backup AS 
                SELECT * FROM products
                """
            )

            # Create the new products table
            await self._connection.execute("DROP TABLE IF EXISTS products_new")

            await self._connection.execute(
                """
                CREATE TABLE products_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    main_category TEXT NOT NULL DEFAULT 'Legacy',
                    sub_category TEXT NOT NULL DEFAULT 'Service',
                    service_name TEXT NOT NULL DEFAULT 'Legacy Product',
                    variant_name TEXT NOT NULL,
                    price_cents INTEGER NOT NULL,
                    start_time TEXT,
                    duration TEXT,
                    refill_period TEXT,
                    additional_info TEXT,
                    role_id INTEGER,
                    content_payload TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Migrate data from old table to new table
            await self._connection.execute(
                """
                INSERT INTO products_new (
                    id, variant_name, price_cents, role_id, content_payload, 
                    is_active, created_at, updated_at
                )
                SELECT 
                    id, name, price_cents, role_id, content_payload,
                    is_active, created_at, updated_at
                FROM products
                """
            )

            # Drop old table and rename new one
            await self._connection.execute("DROP TABLE products")
            await self._connection.execute("ALTER TABLE products_new RENAME TO products")

            await self._connection.commit()
            logger.info("Products table migration completed successfully")

    async def _migration_v3(self) -> None:
        """Migration v3: Create indexes for discounts and orders tables."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_discounts_expires_at
                ON discounts(expires_at)
            """
        )
        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tickets_user_status
                ON tickets(user_discord_id, status)
            """
        )
        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_user
                ON orders(user_discord_id)
            """
        )
        await self._connection.commit()

    async def _migration_v4(self) -> None:
        """Migration v4: Extend tickets table with type, order_id, assigned_staff_id, closed_at, and priority columns."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute("PRAGMA table_info(tickets)")
        columns = [row[1] for row in await cursor.fetchall()]

        if "type" not in columns:
            await self._connection.execute(
                "ALTER TABLE tickets ADD COLUMN type TEXT NOT NULL DEFAULT 'support'"
            )

        if "order_id" not in columns:
            await self._connection.execute(
                "ALTER TABLE tickets ADD COLUMN order_id INTEGER"
            )

        if "assigned_staff_id" not in columns:
            await self._connection.execute(
                "ALTER TABLE tickets ADD COLUMN assigned_staff_id INTEGER"
            )

        if "closed_at" not in columns:
            await self._connection.execute(
                "ALTER TABLE tickets ADD COLUMN closed_at TIMESTAMP"
            )

        if "priority" not in columns:
            await self._connection.execute(
                "ALTER TABLE tickets ADD COLUMN priority TEXT"
            )

        await self._connection.commit()

    async def _migration_v5(self) -> None:
        """Migration v5: Create wallet_transactions table for ledger tracking."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_discord_id INTEGER NOT NULL,
                amount_cents INTEGER NOT NULL,
                balance_after_cents INTEGER NOT NULL,
                transaction_type TEXT NOT NULL,
                description TEXT,
                order_id INTEGER,
                ticket_id INTEGER,
                staff_discord_id INTEGER,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_discord_id) REFERENCES users(discord_id) ON DELETE CASCADE,
                FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE SET NULL,
                FOREIGN KEY(ticket_id) REFERENCES tickets(id) ON DELETE SET NULL
            )
            """
        )

        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_wallet_transactions_user
                ON wallet_transactions(user_discord_id, created_at DESC)
            """
        )

        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_wallet_transactions_type
                ON wallet_transactions(transaction_type)
            """
        )

        await self._connection.commit()

    async def _migration_v6(self) -> None:
        """Migration v6: Extend orders table with status and warranty fields."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute("PRAGMA table_info(orders)")
        columns = [row[1] for row in await cursor.fetchall()]

        if "status" not in columns:
            await self._connection.execute(
                "ALTER TABLE orders ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
            )

        if "warranty_expires_at" not in columns:
            await self._connection.execute(
                "ALTER TABLE orders ADD COLUMN warranty_expires_at TIMESTAMP"
            )

        if "last_renewed_at" not in columns:
            await self._connection.execute(
                "ALTER TABLE orders ADD COLUMN last_renewed_at TIMESTAMP"
            )

        if "renewal_count" not in columns:
            await self._connection.execute(
                "ALTER TABLE orders ADD COLUMN renewal_count INTEGER NOT NULL DEFAULT 0"
            )

        # Create indexes for the new fields
        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_status
                ON orders(status)
            """
        )

        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_warranty_expiry
                ON orders(warranty_expires_at)
            """
        )

        await self._connection.commit()

    async def _migration_v7(self) -> None:
        """Migration v7: Create transcripts table for transcript persistence."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS transcripts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                user_discord_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                storage_type TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                file_size_bytes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(ticket_id) REFERENCES tickets(id) ON DELETE CASCADE
            )
            """
        )

        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transcripts_ticket
                ON transcripts(ticket_id)
            """
        )

        await self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transcripts_user
                ON transcripts(user_discord_id)
            """
        )

        await self._connection.commit()

    async def ensure_user(self, discord_id: int) -> aiosqlite.Row:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            """
            INSERT INTO users (discord_id)
            VALUES (?)
            ON CONFLICT(discord_id) DO NOTHING
            """,
            (discord_id,),
        )
        await self._connection.commit()
        row = await self.get_user(discord_id)
        if row is None:
            raise RuntimeError("Failed to create or retrieve user record.")
        return row

    async def get_user(self, discord_id: int) -> Optional[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM users WHERE discord_id = ?",
            (discord_id,),
        )
        return await cursor.fetchone()

    async def update_wallet_balance(self, discord_id: int, delta_cents: int) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        async with self._wallet_lock:
            await self._connection.execute("BEGIN IMMEDIATE;")
            await self._connection.execute(
                """
                INSERT INTO users (discord_id, wallet_balance_cents)
                VALUES (?, 0)
                ON CONFLICT(discord_id) DO NOTHING;
                """,
                (discord_id,),
            )
            await self._connection.execute(
                """
                UPDATE users
                SET wallet_balance_cents = wallet_balance_cents + ?,
                    total_lifetime_spent_cents = CASE
                        WHEN ? > 0 THEN total_lifetime_spent_cents + ?
                        ELSE total_lifetime_spent_cents
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_id = ?;
                """,
                (delta_cents, delta_cents, delta_cents, discord_id),
            )
            await self._connection.commit()

            cursor = await self._connection.execute(
                "SELECT wallet_balance_cents FROM users WHERE discord_id = ?",
                (discord_id,),
            )
            row = await cursor.fetchone()
            return row["wallet_balance_cents"] if row else 0

    async def log_wallet_transaction(
        self,
        *,
        user_discord_id: int,
        amount_cents: int,
        balance_after_cents: int,
        transaction_type: str,
        description: Optional[str] = None,
        order_id: Optional[int] = None,
        ticket_id: Optional[int] = None,
        staff_discord_id: Optional[int] = None,
        metadata: Optional[str] = None,
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO wallet_transactions (
                user_discord_id, amount_cents, balance_after_cents,
                transaction_type, description, order_id, ticket_id,
                staff_discord_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_discord_id,
                amount_cents,
                balance_after_cents,
                transaction_type,
                description,
                order_id,
                ticket_id,
                staff_discord_id,
                metadata,
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def get_wallet_transactions(
        self,
        user_discord_id: int,
        *,
        limit: int = 10,
        offset: int = 0,
    ) -> list[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT * FROM wallet_transactions
            WHERE user_discord_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (user_discord_id, limit, offset),
        )
        return await cursor.fetchall()

    async def count_wallet_transactions(self, user_discord_id: int) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT COUNT(*) as count FROM wallet_transactions
            WHERE user_discord_id = ?
            """,
            (user_discord_id,),
        )
        row = await cursor.fetchone()
        return row["count"] if row else 0

    async def create_product(
        self,
        *,
        main_category: str,
        sub_category: str,
        service_name: str,
        variant_name: str,
        price_cents: int,
        start_time: Optional[str] = None,
        duration: Optional[str] = None,
        refill_period: Optional[str] = None,
        additional_info: Optional[str] = None,
        role_id: Optional[int] = None,
        content_payload: Optional[str] = None,
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO products (
                main_category, sub_category, service_name, variant_name, 
                price_cents, start_time, duration, refill_period, additional_info,
                role_id, content_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                main_category, sub_category, service_name, variant_name,
                price_cents, start_time, duration, refill_period, additional_info,
                role_id, content_payload
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def create_product_legacy(
        self,
        name: str,
        price_cents: int,
        role_id: Optional[int],
        content_payload: Optional[str],
    ) -> int:
        """Legacy method for backward compatibility."""
        return await self.create_product(
            main_category="Legacy",
            sub_category="Service", 
            service_name="Legacy Product",
            variant_name=name,
            price_cents=price_cents,
            role_id=role_id,
            content_payload=content_payload,
        )

    async def get_product(self, product_id: int) -> Optional[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM products WHERE id = ?",
            (product_id,),
        )
        return await cursor.fetchone()

    async def set_discount(
        self,
        *,
        user_id: Optional[int],
        product_id: Optional[int],
        vip_tier: Optional[str],
        discount_percent: float,
        description: str,
        expires_at: Optional[str] = None,
        is_stackable: bool = False,
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO discounts (
                user_id, product_id, vip_tier, discount_percent, description, expires_at, is_stackable
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                product_id,
                vip_tier,
                discount_percent,
                description,
                expires_at,
                1 if is_stackable else 0,
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def get_applicable_discounts(
        self,
        *,
        user_id: Optional[int],
        product_id: Optional[int],
        vip_tier: Optional[str],
    ) -> list[aiosqlite.Row]:
        """Get applicable discounts, filtering out expired ones.
        
        Returns discounts that match the user, product, and VIP tier criteria,
        but only those that haven't expired (expires_at is NULL or in the future).
        """
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT * FROM discounts
            WHERE (user_id IS NULL OR user_id = ?)
              AND (product_id IS NULL OR product_id = ?)
              AND (vip_tier IS NULL OR vip_tier = ?)
              AND (expires_at IS NULL OR expires_at >= CURRENT_TIMESTAMP)
            """,
            (user_id, product_id, vip_tier),
        )
        return await cursor.fetchall()

    async def get_all_products(self, *, active_only: bool = True) -> list[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        query = "SELECT * FROM products"
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY price_cents ASC"

        cursor = await self._connection.execute(query)
        return await cursor.fetchall()

    async def get_distinct_main_categories(self) -> list[str]:
        """Get all distinct main_category values from active products, sorted alphabetically."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT DISTINCT main_category 
            FROM products 
            WHERE is_active = 1 
            ORDER BY main_category ASC
            """
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def get_distinct_sub_categories(self, main_category: str) -> list[str]:
        """Get all distinct sub_category values for a main_category from active products, sorted alphabetically."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT DISTINCT sub_category 
            FROM products 
            WHERE main_category = ? AND is_active = 1 
            ORDER BY sub_category ASC
            """,
            (main_category,),
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def get_products_by_category(
        self, main_category: str, sub_category: str
    ) -> list[aiosqlite.Row]:
        """Get all active products for a specific main_category and sub_category combination."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT * FROM products 
            WHERE main_category = ? AND sub_category = ? AND is_active = 1 
            ORDER BY service_name ASC, variant_name ASC
            """,
            (main_category, sub_category),
        )
        return await cursor.fetchall()

    async def find_product_by_fields(
        self,
        *,
        main_category: str,
        sub_category: str,
        service_name: str,
        variant_name: str,
    ) -> Optional[aiosqlite.Row]:
        """Find a product by matching the four key fields."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT * FROM products 
            WHERE main_category = ? 
              AND sub_category = ? 
              AND service_name = ? 
              AND variant_name = ?
            """,
            (main_category, sub_category, service_name, variant_name),
        )
        return await cursor.fetchone()

    async def update_product(
        self,
        product_id: int,
        *,
        main_category: Optional[str] = None,
        sub_category: Optional[str] = None,
        service_name: Optional[str] = None,
        variant_name: Optional[str] = None,
        price_cents: Optional[int] = None,
        start_time: Optional[str] = None,
        duration: Optional[str] = None,
        refill_period: Optional[str] = None,
        additional_info: Optional[str] = None,
        role_id: Optional[int] = None,
        content_payload: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> None:
        """Update an existing product with the provided fields."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        # Build dynamic update query
        update_fields = []
        params = []
        
        field_mapping = {
            'main_category': main_category,
            'sub_category': sub_category,
            'service_name': service_name,
            'variant_name': variant_name,
            'price_cents': price_cents,
            'start_time': start_time,
            'duration': duration,
            'refill_period': refill_period,
            'additional_info': additional_info,
            'role_id': role_id,
            'content_payload': content_payload,
            'is_active': 1 if is_active else 0 if is_active is not None else None,
        }
        
        for field, value in field_mapping.items():
            if value is not None:
                update_fields.append(f"{field} = ?")
                params.append(value)
        
        if not update_fields:
            return  # Nothing to update
        
        # Add updated_at timestamp
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        params.append(product_id)
        
        query = f"""
            UPDATE products 
            SET {', '.join(update_fields)}
            WHERE id = ?
        """
        
        await self._connection.execute(query, params)
        await self._connection.commit()

    async def deactivate_all_products_except(
        self,
        active_product_ids: list[int],
    ) -> int:
        """Mark all products as inactive except those in the provided list."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        if not active_product_ids:
            # If no active products, deactivate all
            cursor = await self._connection.execute(
                "UPDATE products SET is_active = 0, updated_at = CURRENT_TIMESTAMP"
            )
        else:
            # Deactivate all except the specified IDs
            placeholders = ','.join('?' for _ in active_product_ids)
            cursor = await self._connection.execute(
                f"""
                UPDATE products 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE id NOT IN ({placeholders})
                """,
                active_product_ids,
            )
        
        await self._connection.commit()
        return cursor.rowcount

    async def mark_client_role_assigned(self, discord_id: int) -> None:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            """
            UPDATE users
            SET has_client_role = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE discord_id = ?
            """,
            (discord_id,),
        )
        await self._connection.commit()

    async def get_manually_assigned_roles(self, discord_id: int) -> list[str]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT manually_assigned_roles FROM users WHERE discord_id = ?",
            (discord_id,),
        )
        row = await cursor.fetchone()
        if not row or not row["manually_assigned_roles"]:
            return []

        import json
        try:
            return json.loads(row["manually_assigned_roles"])
        except (json.JSONDecodeError, TypeError):
            return []

    async def add_manually_assigned_role(self, discord_id: int, role_name: str) -> None:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        import json
        
        async with self._wallet_lock:
            cursor = await self._connection.execute(
                "SELECT manually_assigned_roles FROM users WHERE discord_id = ?",
                (discord_id,),
            )
            row = await cursor.fetchone()
            
            roles = []
            if row and row["manually_assigned_roles"]:
                try:
                    roles = json.loads(row["manually_assigned_roles"])
                except (json.JSONDecodeError, TypeError):
                    roles = []
            
            if role_name not in roles:
                roles.append(role_name)
            
            await self._connection.execute(
                """
                UPDATE users
                SET manually_assigned_roles = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_id = ?
                """,
                (json.dumps(roles), discord_id),
            )
            await self._connection.commit()

    async def remove_manually_assigned_role(self, discord_id: int, role_name: str) -> None:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        import json
        
        async with self._wallet_lock:
            cursor = await self._connection.execute(
                "SELECT manually_assigned_roles FROM users WHERE discord_id = ?",
                (discord_id,),
            )
            row = await cursor.fetchone()
            
            roles = []
            if row and row["manually_assigned_roles"]:
                try:
                    roles = json.loads(row["manually_assigned_roles"])
                except (json.JSONDecodeError, TypeError):
                    roles = []
            
            if role_name in roles:
                roles.remove(role_name)
            
            await self._connection.execute(
                """
                UPDATE users
                SET manually_assigned_roles = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_id = ?
                """,
                (json.dumps(roles) if roles else None, discord_id),
            )
            await self._connection.commit()

    async def create_ticket(
        self,
        *,
        user_discord_id: int,
        channel_id: int,
        status: str = "open",
        ticket_type: str = "support",
        order_id: Optional[int] = None,
        assigned_staff_id: Optional[int] = None,
        priority: Optional[str] = None,
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO tickets (
                user_discord_id, channel_id, status, type, order_id, 
                assigned_staff_id, priority
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_discord_id,
                channel_id,
                status,
                ticket_type,
                order_id,
                assigned_staff_id,
                priority,
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def get_open_ticket_for_user(self, user_discord_id: int) -> Optional[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT *
            FROM tickets
            WHERE user_discord_id = ? AND status = 'open'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_discord_id,),
        )
        return await cursor.fetchone()

    async def get_ticket_by_channel(self, channel_id: int) -> Optional[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM tickets WHERE channel_id = ?",
            (channel_id,),
        )
        return await cursor.fetchone()

    async def update_ticket_status(
        self, channel_id: int, status: str, *, update_activity: bool = False
    ) -> None:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        if update_activity:
            await self._connection.execute(
                """
                UPDATE tickets
                SET status = ?,
                    last_activity = CURRENT_TIMESTAMP
                WHERE channel_id = ?
                """,
                (status, channel_id),
            )
        else:
            await self._connection.execute(
                """
                UPDATE tickets
                SET status = ?
                WHERE channel_id = ?
                """,
                (status, channel_id),
            )
        await self._connection.commit()

    async def touch_ticket_activity(self, channel_id: int) -> None:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute(
            """
            UPDATE tickets
            SET last_activity = CURRENT_TIMESTAMP
            WHERE channel_id = ?
            """,
            (channel_id,),
        )
        await self._connection.commit()

    async def get_open_tickets(self) -> list[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT *
            FROM tickets
            WHERE status = 'open'
            ORDER BY last_activity ASC
            """
        )
        return await cursor.fetchall()

    async def update_ticket(
        self,
        channel_id: int,
        *,
        status: Optional[str] = None,
        ticket_type: Optional[str] = None,
        order_id: Optional[int] = None,
        assigned_staff_id: Optional[int] = None,
        priority: Optional[str] = None,
        closed_at: Optional[str] = None,
    ) -> None:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        update_fields = []
        params = []

        if status is not None:
            update_fields.append("status = ?")
            params.append(status)

        if ticket_type is not None:
            update_fields.append("type = ?")
            params.append(ticket_type)

        if order_id is not None:
            update_fields.append("order_id = ?")
            params.append(order_id)

        if assigned_staff_id is not None:
            update_fields.append("assigned_staff_id = ?")
            params.append(assigned_staff_id)

        if priority is not None:
            update_fields.append("priority = ?")
            params.append(priority)

        if closed_at is not None:
            update_fields.append("closed_at = ?")
            params.append(closed_at)

        if not update_fields:
            return

        params.append(channel_id)
        query = f"""
            UPDATE tickets
            SET {', '.join(update_fields)}
            WHERE channel_id = ?
        """

        await self._connection.execute(query, params)
        await self._connection.commit()

    async def create_order(
        self,
        *,
        user_discord_id: int,
        product_id: int,
        price_paid_cents: int,
        discount_applied_percent: float,
        order_metadata: Optional[str] = None,
        status: str = "pending",
        warranty_expires_at: Optional[str] = None,
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO orders (
                user_discord_id, product_id, price_paid_cents, 
                discount_applied_percent, order_metadata, status,
                warranty_expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_discord_id,
                product_id,
                price_paid_cents,
                discount_applied_percent,
                order_metadata,
                status,
                warranty_expires_at,
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def create_manual_order(
        self,
        *,
        user_discord_id: int,
        product_name: str,
        price_paid_cents: int,
        notes: Optional[str] = None,
    ) -> tuple[int, int]:
        """
        Create a manual order without affecting wallet balance.
        
        Args:
            user_discord_id: Discord user ID
            product_name: Name of the product (for logging)
            price_paid_cents: Amount paid in cents
            notes: Additional notes about the order
        
        Returns:
            Tuple of (order_id, new_lifetime_spend_cents)
        """
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        async with self._wallet_lock:
            await self._connection.execute("BEGIN IMMEDIATE;")
            
            # Ensure user exists
            await self.ensure_user(user_discord_id)
            
            # Update lifetime spend only (not wallet balance)
            await self._connection.execute(
                """
                UPDATE users
                SET total_lifetime_spent_cents = total_lifetime_spent_cents + ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_id = ?
                """,
                (price_paid_cents, user_discord_id),
            )
            
            # Create order record with a dummy product_id (0 for manual orders)
            import json
            order_metadata = json.dumps({
                "product_name": product_name,
                "manual_order": True,
                "notes": notes,
            })
            
            cursor = await self._connection.execute(
                """
                INSERT INTO orders (
                    user_discord_id, product_id, price_paid_cents,
                    discount_applied_percent, order_metadata, status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_discord_id,
                    0,  # product_id = 0 for manual orders
                    price_paid_cents,
                    0.0,  # No discount for manual orders
                    order_metadata,
                    "pending",  # Default status for manual orders
                ),
            )
            order_id = cursor.lastrowid
            
            await self._connection.commit()
            
            # Get updated lifetime spend
            cursor = await self._connection.execute(
                "SELECT total_lifetime_spent_cents FROM users WHERE discord_id = ?",
                (user_discord_id,),
            )
            row = await cursor.fetchone()
            new_lifetime_spend = row["total_lifetime_spent_cents"] if row else 0
            
            return order_id, new_lifetime_spend

    async def bulk_upsert_products(
        self,
        products_to_add: list[dict],
        products_to_update: list[dict],
        product_ids_to_keep_active: list[int],
    ) -> tuple[int, int, int]:
        """Perform bulk insert/update/deactivate of products in a single transaction.

        Args:
            products_to_add: List of dicts with fields for new products
            products_to_update: List of dicts with 'id' and update fields
            product_ids_to_keep_active: List of product IDs to keep active (others deactivated)

        Returns:
            Tuple of (added_count, updated_count, deactivated_count)
        """
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute("BEGIN IMMEDIATE;")
        try:
            added_count = 0
            updated_count = 0
            deactivated_count = 0

            for product in products_to_add:
                await self._connection.execute(
                    """
                    INSERT INTO products (
                        main_category, sub_category, service_name, variant_name,
                        price_cents, start_time, duration, refill_period, additional_info,
                        role_id, content_payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        product.get('main_category'),
                        product.get('sub_category'),
                        product.get('service_name'),
                        product.get('variant_name'),
                        product.get('price_cents'),
                        product.get('start_time'),
                        product.get('duration'),
                        product.get('refill_period'),
                        product.get('additional_info'),
                        product.get('role_id'),
                        product.get('content_payload'),
                    ),
                )
                added_count += 1

            for product in products_to_update:
                product_id = product.pop('id')
                update_fields = []
                params = []

                for field, value in product.items():
                    if value is not None:
                        update_fields.append(f"{field} = ?")
                        params.append(value)

                if update_fields:
                    update_fields.append("updated_at = CURRENT_TIMESTAMP")
                    params.append(product_id)

                    query = f"""
                        UPDATE products
                        SET {', '.join(update_fields)}
                        WHERE id = ?
                    """
                    await self._connection.execute(query, params)
                    updated_count += 1

            if not product_ids_to_keep_active:
                cursor = await self._connection.execute(
                    "UPDATE products SET is_active = 0, updated_at = CURRENT_TIMESTAMP"
                )
            else:
                placeholders = ','.join('?' for _ in product_ids_to_keep_active)
                cursor = await self._connection.execute(
                    f"""
                    UPDATE products
                    SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                    WHERE id NOT IN ({placeholders})
                    """,
                    product_ids_to_keep_active,
                )
            deactivated_count = cursor.rowcount

            await self._connection.commit()
            return added_count, updated_count, deactivated_count
        except Exception:
            await self._connection.rollback()
            raise

    async def purchase_product(
        self,
        *,
        user_discord_id: int,
        product_id: int,
        price_paid_cents: int,
        discount_applied_percent: float,
        order_metadata: Optional[str] = None,
    ) -> tuple[int, int]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        async with self._wallet_lock:
            await self._connection.execute("BEGIN IMMEDIATE;")

            cursor = await self._connection.execute(
                "SELECT wallet_balance_cents FROM users WHERE discord_id = ?",
                (user_discord_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                await self._connection.rollback()
                raise ValueError("User not found")

            current_balance = row["wallet_balance_cents"]
            if current_balance < price_paid_cents:
                await self._connection.rollback()
                raise ValueError("Insufficient balance")

            await self._connection.execute(
                """
                UPDATE users
                SET wallet_balance_cents = wallet_balance_cents - ?,
                    total_lifetime_spent_cents = total_lifetime_spent_cents + ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_id = ?
                """,
                (price_paid_cents, price_paid_cents, user_discord_id),
            )

            cursor = await self._connection.execute(
                """
                INSERT INTO orders (
                    user_discord_id, product_id, price_paid_cents,
                    discount_applied_percent, order_metadata, status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_discord_id,
                    product_id,
                    price_paid_cents,
                    discount_applied_percent,
                    order_metadata,
                    "pending",  # Default status for new orders
                ),
            )
            order_id = cursor.lastrowid

            cursor = await self._connection.execute(
                "SELECT wallet_balance_cents FROM users WHERE discord_id = ?",
                (user_discord_id,),
            )
            row = await cursor.fetchone()
            new_balance = row["wallet_balance_cents"] if row else 0

            await self._connection.execute(
                """
                INSERT INTO wallet_transactions (
                    user_discord_id, amount_cents, balance_after_cents,
                    transaction_type, description, order_id, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_discord_id,
                    -price_paid_cents,
                    new_balance,
                    "purchase",
                    f"Purchase of product #{product_id}",
                    order_id,
                    order_metadata,
                ),
            )

            await self._connection.commit()

            return order_id, new_balance

    async def get_orders_for_user(
        self,
        user_discord_id: int,
        *,
        limit: int = 10,
        offset: int = 0,
    ) -> list[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT * FROM orders
            WHERE user_discord_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (user_discord_id, limit, offset),
        )
        return await cursor.fetchall()

    async def count_orders_for_user(self, user_discord_id: int) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT COUNT(*) as count FROM orders
            WHERE user_discord_id = ?
            """,
            (user_discord_id,),
        )
        row = await cursor.fetchone()
        return row["count"] if row else 0

    async def get_order_by_id(self, order_id: int) -> Optional[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM orders WHERE id = ?",
            (order_id,),
        )
        return await cursor.fetchone()

    async def get_ticket_by_order_id(self, order_id: int) -> Optional[aiosqlite.Row]:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM tickets WHERE order_id = ?",
            (order_id,),
        )
        return await cursor.fetchone()

    async def update_order_status(self, order_id: int, status: str) -> None:
        """Update the status of an order."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        valid_statuses = ["pending", "fulfilled", "refill", "refunded"]
        if status not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {', '.join(valid_statuses)}")

        await self._connection.execute(
            "UPDATE orders SET status = ? WHERE id = ?",
            (status, order_id),
        )
        await self._connection.commit()

    async def renew_order_warranty(
        self, 
        order_id: int, 
        warranty_expires_at: str,
        staff_discord_id: Optional[int] = None
    ) -> None:
        """Renew an order's warranty and update renewal tracking."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        await self._connection.execute("BEGIN IMMEDIATE;")
        
        try:
            # Update warranty and renewal info
            await self._connection.execute(
                """
                UPDATE orders 
                SET warranty_expires_at = ?,
                    last_renewed_at = CURRENT_TIMESTAMP,
                    renewal_count = renewal_count + 1
                WHERE id = ?
                """,
                (warranty_expires_at, order_id),
            )

            # Log the renewal in wallet transactions
            cursor = await self._connection.execute(
                "SELECT user_discord_id FROM orders WHERE id = ?",
                (order_id,),
            )
            order_row = await cursor.fetchone()
            if order_row:
                await self.log_wallet_transaction(
                    user_discord_id=order_row["user_discord_id"],
                    amount_cents=0,
                    balance_after_cents=0,  # Not applicable for warranty renewal
                    transaction_type="warranty_renewal",
                    description=f"Warranty renewal for order #{order_id}",
                    order_id=order_id,
                    staff_discord_id=staff_discord_id,
                    metadata=f'{{"renewal_date": "{warranty_expires_at}"}}',
                )

            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise

    async def get_orders_expiring_soon(self, days_ahead: int = 7) -> list[aiosqlite.Row]:
        """Get orders with warranties expiring within the specified number of days."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT o.*, u.discord_id as user_discord_id
            FROM orders o
            JOIN users u ON o.user_discord_id = u.discord_id
            WHERE o.warranty_expires_at IS NOT NULL
              AND o.warranty_expires_at <= datetime('now', '+' || ? || ' days')
              AND o.warranty_expires_at > datetime('now')
              AND o.status IN ('fulfilled', 'refill')
            ORDER BY o.warranty_expires_at ASC
            """,
            (days_ahead,),
        )
        return await cursor.fetchall()

    async def get_active_orders(self, user_discord_id: Optional[int] = None) -> list[aiosqlite.Row]:
        """Get orders that are currently active (not refunded)."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        if user_discord_id:
            cursor = await self._connection.execute(
                """
                SELECT * FROM orders
                WHERE user_discord_id = ? AND status != 'refunded'
                ORDER BY created_at DESC
                """,
                (user_discord_id,),
            )
        else:
            cursor = await self._connection.execute(
                """
                SELECT * FROM orders
                WHERE status != 'refunded'
                ORDER BY created_at DESC
                """
            )
        return await cursor.fetchall()

    async def save_transcript(
        self,
        *,
        ticket_id: int,
        user_discord_id: int,
        channel_id: int,
        storage_type: str,
        storage_path: str,
        file_size_bytes: Optional[int] = None,
    ) -> int:
        """Save transcript metadata to the database."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO transcripts (
                ticket_id, user_discord_id, channel_id,
                storage_type, storage_path, file_size_bytes
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ticket_id, user_discord_id, channel_id, storage_type, storage_path, file_size_bytes),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def get_transcript_by_ticket_id(self, ticket_id: int) -> Optional[aiosqlite.Row]:
        """Get transcript metadata for a specific ticket."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM transcripts WHERE ticket_id = ? ORDER BY created_at DESC LIMIT 1",
            (ticket_id,),
        )
        return await cursor.fetchone()

    async def get_transcripts_by_user(self, user_discord_id: int) -> list[aiosqlite.Row]:
        """Get all transcript metadata for a specific user."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            "SELECT * FROM transcripts WHERE user_discord_id = ? ORDER BY created_at DESC",
            (user_discord_id,),
        )
        return await cursor.fetchall()
