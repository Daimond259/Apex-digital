import json
from datetime import datetime, timedelta, timezone

import pytest


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
