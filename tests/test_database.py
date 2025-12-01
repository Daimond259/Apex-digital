import json
from datetime import datetime, timedelta, timezone

import pytest
import aiosqlite


@pytest.mark.asyncio
async def test_update_wallet_balance_tracks_lifetime_spend(db):
    balance = await db.update_wallet_balance(12345, 500)
    assert balance == 500

    balance = await db.update_wallet_balance(12345, 250)
    assert balance == 750

    user = await db.get_user(12345)
    assert user is not None
    assert user["wallet_balance_cents"] == 750
    assert user["total_lifetime_spent_cents"] == 750


@pytest.mark.asyncio
async def test_update_wallet_balance_negative_deltas_do_not_increase_lifetime(db):
    await db.update_wallet_balance(23456, 1000)
    await db.update_wallet_balance(23456, -400)

    user = await db.get_user(23456)
    assert user is not None
    assert user["wallet_balance_cents"] == 600
    assert user["total_lifetime_spent_cents"] == 1000


@pytest.mark.asyncio
async def test_purchase_product_deducts_balance_and_creates_order(db):
    await db.ensure_user(34567)
    await db.update_wallet_balance(34567, 2_000)

    product_id = await db.create_product(
        main_category="Test",
        sub_category="Digital",
        service_name="Bundle",
        variant_name="Premium",
        price_cents=1_500,
    )

    order_id, new_balance = await db.purchase_product(
        user_discord_id=34567,
        product_id=product_id,
        price_paid_cents=1_200,
        discount_applied_percent=10.0,
        order_metadata="{}",
    )

    assert new_balance == 800

    cursor = await db._connection.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
    order = await cursor.fetchone()
    assert order is not None
    assert order["user_discord_id"] == 34567
    assert order["product_id"] == product_id
    assert order["price_paid_cents"] == 1_200
    assert order["discount_applied_percent"] == 10.0

    user = await db.get_user(34567)
    assert user is not None
    assert user["wallet_balance_cents"] == 800
    assert user["total_lifetime_spent_cents"] == 3_200


@pytest.mark.asyncio
async def test_purchase_product_with_insufficient_funds_raises(db):
    await db.ensure_user(45678)
    await db.update_wallet_balance(45678, 300)

    product_id = await db.create_product(
        main_category="Test",
        sub_category="Digital",
        service_name="Bundle",
        variant_name="Basic",
        price_cents=500,
    )

    with pytest.raises(ValueError, match="Insufficient balance"):
        await db.purchase_product(
            user_discord_id=45678,
            product_id=product_id,
            price_paid_cents=400,
            discount_applied_percent=0.0,
        )

    user = await db.get_user(45678)
    assert user is not None
    assert user["wallet_balance_cents"] == 300


@pytest.mark.asyncio
async def test_get_applicable_discounts_skips_expired_entries(db):
    user_row = await db.ensure_user(56789)
    await db.ensure_user(67890)

    future = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    past = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    active_discount_id = await db.set_discount(
        user_id=user_row["id"],
        product_id=None,
        vip_tier=None,
        discount_percent=15.0,
        description="Active",
        expires_at=future,
    )

    await db.set_discount(
        user_id=user_row["id"],
        product_id=None,
        vip_tier=None,
        discount_percent=50.0,
        description="Expired",
        expires_at=past,
    )

    discounts = await db.get_applicable_discounts(
        user_id=user_row["id"],
        product_id=None,
        vip_tier=None,
    )

    assert len(discounts) == 1
    assert discounts[0]["id"] == active_discount_id


@pytest.mark.asyncio
async def test_create_manual_order_updates_lifetime_not_wallet(db):
    await db.ensure_user(78901)
    await db.update_wallet_balance(78901, 1_000)

    order_id, new_lifetime = await db.create_manual_order(
        user_discord_id=78901,
        product_name="Support Package",
        price_paid_cents=400,
        notes="Manual entry",
    )

    assert new_lifetime == 1_400

    user = await db.get_user(78901)
    assert user is not None
    assert user["wallet_balance_cents"] == 1_000
    assert user["total_lifetime_spent_cents"] == 1_400

    cursor = await db._connection.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
    order = await cursor.fetchone()
    assert order is not None
    assert order["product_id"] == 0
    metadata = json.loads(order["order_metadata"])
    assert metadata["manual_order"] is True
    assert metadata["product_name"] == "Support Package"
    assert metadata["notes"] == "Manual entry"


@pytest.mark.asyncio
async def test_migration_v4_extends_tickets_table(db):
    """Test that migration v4 adds new columns to tickets table."""
    cursor = await db._connection.execute("PRAGMA table_info(tickets)")
    columns = {row[1]: row for row in await cursor.fetchall()}

    assert "type" in columns
    assert "order_id" in columns
    assert "assigned_staff_id" in columns
    assert "closed_at" in columns
    assert "priority" in columns


@pytest.mark.asyncio
async def test_create_ticket_with_defaults(db):
    """Test creating a ticket with default values for new fields."""
    await db.ensure_user(11111)
    
    ticket_id = await db.create_ticket(
        user_discord_id=11111,
        channel_id=22222,
    )

    assert ticket_id > 0

    ticket = await db.get_ticket_by_channel(22222)
    assert ticket is not None
    assert ticket["user_discord_id"] == 11111
    assert ticket["channel_id"] == 22222
    assert ticket["status"] == "open"
    assert ticket["type"] == "support"
    assert ticket["order_id"] is None
    assert ticket["assigned_staff_id"] is None
    assert ticket["priority"] is None
    assert ticket["closed_at"] is None


@pytest.mark.asyncio
async def test_create_ticket_with_all_fields(db):
    """Test creating a ticket with all fields specified."""
    await db.ensure_user(33333)
    
    ticket_id = await db.create_ticket(
        user_discord_id=33333,
        channel_id=44444,
        status="open",
        ticket_type="billing",
        order_id=999,
        assigned_staff_id=555,
        priority="high",
    )

    assert ticket_id > 0

    ticket = await db.get_ticket_by_channel(44444)
    assert ticket is not None
    assert ticket["user_discord_id"] == 33333
    assert ticket["status"] == "open"
    assert ticket["type"] == "billing"
    assert ticket["order_id"] == 999
    assert ticket["assigned_staff_id"] == 555
    assert ticket["priority"] == "high"


@pytest.mark.asyncio
async def test_update_ticket_type(db):
    """Test updating ticket type field."""
    await db.ensure_user(55555)
    
    ticket_id = await db.create_ticket(
        user_discord_id=55555,
        channel_id=66666,
    )

    await db.update_ticket(66666, ticket_type="sales")

    ticket = await db.get_ticket_by_channel(66666)
    assert ticket is not None
    assert ticket["type"] == "sales"
    assert ticket["status"] == "open"


@pytest.mark.asyncio
async def test_update_ticket_assigned_staff(db):
    """Test updating ticket assigned_staff_id field."""
    await db.ensure_user(77777)
    
    ticket_id = await db.create_ticket(
        user_discord_id=77777,
        channel_id=88888,
    )

    await db.update_ticket(88888, assigned_staff_id=777)

    ticket = await db.get_ticket_by_channel(88888)
    assert ticket is not None
    assert ticket["assigned_staff_id"] == 777


@pytest.mark.asyncio
async def test_update_ticket_priority(db):
    """Test updating ticket priority field."""
    await db.ensure_user(99999)
    
    ticket_id = await db.create_ticket(
        user_discord_id=99999,
        channel_id=100000,
    )

    await db.update_ticket(100000, priority="critical")

    ticket = await db.get_ticket_by_channel(100000)
    assert ticket is not None
    assert ticket["priority"] == "critical"


@pytest.mark.asyncio
async def test_update_ticket_order_id(db):
    """Test updating ticket order_id field."""
    await db.ensure_user(111111)
    
    ticket_id = await db.create_ticket(
        user_discord_id=111111,
        channel_id=122222,
    )

    await db.update_ticket(122222, order_id=1234)

    ticket = await db.get_ticket_by_channel(122222)
    assert ticket is not None
    assert ticket["order_id"] == 1234


@pytest.mark.asyncio
async def test_update_ticket_closed_at(db):
    """Test updating ticket closed_at timestamp field."""
    await db.ensure_user(133333)
    
    ticket_id = await db.create_ticket(
        user_discord_id=133333,
        channel_id=144444,
    )

    closed_timestamp = "2024-12-01 10:30:45"
    await db.update_ticket(144444, closed_at=closed_timestamp)

    ticket = await db.get_ticket_by_channel(144444)
    assert ticket is not None
    assert ticket["closed_at"] == closed_timestamp


@pytest.mark.asyncio
async def test_update_ticket_multiple_fields(db):
    """Test updating multiple ticket fields at once."""
    await db.ensure_user(155555)
    
    ticket_id = await db.create_ticket(
        user_discord_id=155555,
        channel_id=166666,
    )

    await db.update_ticket(
        166666,
        ticket_type="support",
        assigned_staff_id=888,
        priority="medium",
        order_id=5678,
        closed_at="2024-12-01 15:45:30",
    )

    ticket = await db.get_ticket_by_channel(166666)
    assert ticket is not None
    assert ticket["type"] == "support"
    assert ticket["assigned_staff_id"] == 888
    assert ticket["priority"] == "medium"
    assert ticket["order_id"] == 5678
    assert ticket["closed_at"] == "2024-12-01 15:45:30"


@pytest.mark.asyncio
async def test_update_ticket_status_preserves_other_fields(db):
    """Test that updating status doesn't affect new fields."""
    await db.ensure_user(177777)
    
    await db.create_ticket(
        user_discord_id=177777,
        channel_id=188888,
        ticket_type="billing",
        assigned_staff_id=999,
        priority="high",
    )

    await db.update_ticket_status(188888, "resolved")

    ticket = await db.get_ticket_by_channel(188888)
    assert ticket is not None
    assert ticket["status"] == "resolved"
    assert ticket["type"] == "billing"
    assert ticket["assigned_staff_id"] == 999
    assert ticket["priority"] == "high"


@pytest.mark.asyncio
async def test_ticket_fields_persist_across_operations(db):
    """Test that ticket fields persist through multiple operations."""
    await db.ensure_user(199999)
    
    ticket_id = await db.create_ticket(
        user_discord_id=199999,
        channel_id=200000,
        ticket_type="sales",
        order_id=9999,
        priority="low",
    )

    await db.touch_ticket_activity(200000)

    ticket = await db.get_ticket_by_channel(200000)
    assert ticket is not None
    assert ticket["type"] == "sales"
    assert ticket["order_id"] == 9999
    assert ticket["priority"] == "low"


@pytest.mark.asyncio
async def test_migration_v6_extends_orders_table(db):
    """Test that migration v6 adds new fields to orders table."""
    # Check that new columns exist
    cursor = await db._connection.execute("PRAGMA table_info(orders)")
    columns = [row[1] for row in await cursor.fetchall()]
    
    assert "status" in columns
    assert "warranty_expires_at" in columns
    assert "last_renewed_at" in columns
    assert "renewal_count" in columns


@pytest.mark.asyncio
async def test_create_order_with_status_and_warranty(db):
    """Test creating orders with the new status and warranty fields."""
    await db.ensure_user(54321)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Test Service",
        variant_name="Test Variant",
        price_cents=1000,
    )
    
    order_id = await db.create_order(
        user_discord_id=54321,
        product_id=product_id,
        price_paid_cents=800,
        discount_applied_percent=20.0,
        status="pending",
        warranty_expires_at="2024-12-31 23:59:59",
    )
    
    order = await db.get_order_by_id(order_id)
    assert order is not None
    assert order["status"] == "pending"
    assert order["warranty_expires_at"] == "2024-12-31 23:59:59"
    assert order["renewal_count"] == 0


@pytest.mark.asyncio
async def test_update_order_status(db):
    """Test updating order status with validation."""
    await db.ensure_user(65432)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Test Service",
        variant_name="Test Variant",
        price_cents=1000,
    )
    
    order_id = await db.create_order(
        user_discord_id=65432,
        product_id=product_id,
        price_paid_cents=1000,
        discount_applied_percent=0.0,
    )
    
    # Test valid status update
    await db.update_order_status(order_id, "fulfilled")
    order = await db.get_order_by_id(order_id)
    assert order["status"] == "fulfilled"
    
    # Test invalid status raises error
    with pytest.raises(ValueError, match="Invalid status"):
        await db.update_order_status(order_id, "invalid_status")


@pytest.mark.asyncio
async def test_renew_order_warranty(db):
    """Test warranty renewal functionality."""
    await db.ensure_user(76543)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Test Service",
        variant_name="Test Variant",
        price_cents=1000,
    )
    
    order_id = await db.create_order(
        user_discord_id=76543,
        product_id=product_id,
        price_paid_cents=1000,
        discount_applied_percent=0.0,
        status="fulfilled",
        warranty_expires_at="2024-12-31 23:59:59",
    )
    
    # Test warranty renewal
    await db.renew_order_warranty(
        order_id, 
        "2025-12-31 23:59:59", 
        staff_discord_id=999999
    )
    
    order = await db.get_order_by_id(order_id)
    assert order["warranty_expires_at"] == "2025-12-31 23:59:59"
    assert order["renewal_count"] == 1
    assert order["last_renewed_at"] is not None
    
    # Check that warranty renewal was logged in transactions
    transactions = await db.get_wallet_transactions(76543)
    warranty_txn = next(
        (t for t in transactions if t["transaction_type"] == "warranty_renewal"), 
        None
    )
    assert warranty_txn is not None
    assert warranty_txn["order_id"] == order_id
    assert warranty_txn["staff_discord_id"] == 999999


@pytest.mark.asyncio
async def test_get_orders_expiring_soon(db):
    """Test retrieving orders with expiring warranties."""
    # Create user and product
    await db.ensure_user(87654)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Test Service",
        variant_name="Test Variant",
        price_cents=1000,
    )
    
    # Create order with warranty expiring soon
    from datetime import datetime, timedelta
    
    future_date = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    order_id = await db.create_order(
        user_discord_id=87654,
        product_id=product_id,
        price_paid_cents=1000,
        discount_applied_percent=0.0,
        status="fulfilled",
        warranty_expires_at=future_date,
    )
    
    # Test retrieving expiring orders
    expiring_orders = await db.get_orders_expiring_soon(7)  # 7 days ahead
    assert len(expiring_orders) >= 1
    
    expiring_order = next(
        (o for o in expiring_orders if o["id"] == order_id), 
        None
    )
    assert expiring_order is not None
    assert expiring_order["user_discord_id"] == 87654


@pytest.mark.asyncio
async def test_get_active_orders(db):
    """Test retrieving active (non-refunded) orders."""
    await db.ensure_user(98765)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Test Service",
        variant_name="Test Variant",
        price_cents=1000,
    )
    
    # Create multiple orders with different statuses
    order1_id = await db.create_order(
        user_discord_id=98765,
        product_id=product_id,
        price_paid_cents=1000,
        discount_applied_percent=0.0,
        status="fulfilled",
    )
    
    order2_id = await db.create_order(
        user_discord_id=98765,
        product_id=product_id,
        price_paid_cents=1000,
        discount_applied_percent=0.0,
        status="refunded",
    )
    
    # Test active orders for specific user
    active_orders = await db.get_active_orders(98765)
    active_order_ids = [o["id"] for o in active_orders]
    
    assert order1_id in active_order_ids
    assert order2_id not in active_order_ids  # Refunded order should not be included
