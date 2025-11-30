"""Async SQLite data layer for Apex Core."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import aiosqlite


class Database:
    """Async database handler using SQLite."""

    def __init__(self, db_path: str | Path = "apex_core.db") -> None:
        self.db_path = Path(db_path)
        self._connection: Optional[aiosqlite.Connection] = None
        self._wallet_lock = asyncio.Lock()

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
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        # First create the basic schema if it doesn't exist
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

            CREATE INDEX IF NOT EXISTS idx_tickets_user_status
                ON tickets(user_discord_id, status);

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

            CREATE INDEX IF NOT EXISTS idx_orders_user
                ON orders(user_discord_id);
            """
        )
        await self._connection.commit()
        
        # Handle migration for products table
        await self._migrate_products_table()

    async def _migrate_products_table(self) -> None:
        """Migrate products table to new schema if needed."""
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        # Check if the products table has the new columns
        cursor = await self._connection.execute("PRAGMA table_info(products)")
        columns = [row[1] for row in await cursor.fetchall()]
        
        # If the table has the old 'name' column but not the new columns, migrate it
        if 'name' in columns and 'main_category' not in columns:
            logger = __import__('logging').getLogger(__name__)
            logger.info("Migrating products table to new schema...")
            
            # Create a backup of existing products
            await self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS products_backup AS 
                SELECT * FROM products
                """
            )
            
            # Create the new products table
            await self._connection.execute(
                """
                DROP TABLE IF EXISTS products_new
                """
            )
            
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
            logger.info("Products table migration completed successfully.")

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
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            SELECT * FROM discounts
            WHERE (user_id IS NULL OR user_id = ?)
              AND (product_id IS NULL OR product_id = ?)
              AND (vip_tier IS NULL OR vip_tier = ?)
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
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO tickets (user_discord_id, channel_id, status)
            VALUES (?, ?, ?)
            """,
            (user_discord_id, channel_id, status),
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

    async def create_order(
        self,
        *,
        user_discord_id: int,
        product_id: int,
        price_paid_cents: int,
        discount_applied_percent: float,
        order_metadata: Optional[str] = None,
    ) -> int:
        if self._connection is None:
            raise RuntimeError("Database connection not initialized.")

        cursor = await self._connection.execute(
            """
            INSERT INTO orders (
                user_discord_id, product_id, price_paid_cents, 
                discount_applied_percent, order_metadata
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                user_discord_id,
                product_id,
                price_paid_cents,
                discount_applied_percent,
                order_metadata,
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
                    discount_applied_percent, order_metadata
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_discord_id,
                    0,  # product_id = 0 for manual orders
                    price_paid_cents,
                    0.0,  # No discount for manual orders
                    order_metadata,
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
                    discount_applied_percent, order_metadata
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_discord_id,
                    product_id,
                    price_paid_cents,
                    discount_applied_percent,
                    order_metadata,
                ),
            )
            order_id = cursor.lastrowid
            
            await self._connection.commit()
            
            cursor = await self._connection.execute(
                "SELECT wallet_balance_cents FROM users WHERE discord_id = ?",
                (user_discord_id,),
            )
            row = await cursor.fetchone()
            new_balance = row["wallet_balance_cents"] if row else 0
            
            return order_id, new_balance
