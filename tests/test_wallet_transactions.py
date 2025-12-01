import json
import pytest


@pytest.mark.asyncio
async def test_migration_v5_creates_wallet_transactions_table(db):
    """Test that migration v5 creates wallet_transactions table."""
    cursor = await db._connection.execute("PRAGMA table_info(wallet_transactions)")
    columns = {row[1]: row for row in await cursor.fetchall()}

    assert "id" in columns
    assert "user_discord_id" in columns
    assert "amount_cents" in columns
    assert "balance_after_cents" in columns
    assert "transaction_type" in columns
    assert "description" in columns
    assert "order_id" in columns
    assert "ticket_id" in columns
    assert "staff_discord_id" in columns
    assert "metadata" in columns
    assert "created_at" in columns


@pytest.mark.asyncio
async def test_log_wallet_transaction(db):
    """Test logging a wallet transaction."""
    await db.ensure_user(12345)
    
    txn_id = await db.log_wallet_transaction(
        user_discord_id=12345,
        amount_cents=1000,
        balance_after_cents=1000,
        transaction_type="admin_credit",
        description="Test credit",
        staff_discord_id=99999,
    )
    
    assert txn_id > 0
    
    cursor = await db._connection.execute(
        "SELECT * FROM wallet_transactions WHERE id = ?", (txn_id,)
    )
    txn = await cursor.fetchone()
    
    assert txn is not None
    assert txn["user_discord_id"] == 12345
    assert txn["amount_cents"] == 1000
    assert txn["balance_after_cents"] == 1000
    assert txn["transaction_type"] == "admin_credit"
    assert txn["description"] == "Test credit"
    assert txn["staff_discord_id"] == 99999


@pytest.mark.asyncio
async def test_get_wallet_transactions_pagination(db):
    """Test retrieving wallet transactions with pagination."""
    await db.ensure_user(23456)
    
    for i in range(15):
        await db.log_wallet_transaction(
            user_discord_id=23456,
            amount_cents=100 * (i + 1),
            balance_after_cents=100 * (i + 1),
            transaction_type="test",
            description=f"Transaction {i}",
        )
    
    page1 = await db.get_wallet_transactions(23456, limit=10, offset=0)
    assert len(page1) == 10
    
    page2 = await db.get_wallet_transactions(23456, limit=10, offset=10)
    assert len(page2) == 5
    
    total = await db.count_wallet_transactions(23456)
    assert total == 15


@pytest.mark.asyncio
async def test_purchase_product_logs_transaction(db):
    """Test that purchasing a product logs a wallet transaction."""
    await db.ensure_user(34567)
    await db.update_wallet_balance(34567, 2000)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Premium",
        variant_name="Gold",
        price_cents=1500,
    )
    
    order_id, new_balance = await db.purchase_product(
        user_discord_id=34567,
        product_id=product_id,
        price_paid_cents=1200,
        discount_applied_percent=10.0,
        order_metadata=json.dumps({"test": "data"}),
    )
    
    assert new_balance == 800
    
    transactions = await db.get_wallet_transactions(34567, limit=10)
    purchase_txns = [t for t in transactions if t["transaction_type"] == "purchase"]
    
    assert len(purchase_txns) >= 1
    
    latest_purchase = purchase_txns[0]
    assert latest_purchase["user_discord_id"] == 34567
    assert latest_purchase["amount_cents"] == -1200
    assert latest_purchase["balance_after_cents"] == 800
    assert latest_purchase["order_id"] == order_id
    assert "Purchase of product" in latest_purchase["description"]


@pytest.mark.asyncio
async def test_wallet_transaction_with_metadata(db):
    """Test logging a transaction with JSON metadata."""
    await db.ensure_user(45678)
    
    metadata = json.dumps({"proof": "txn_abc123", "source": "binance"})
    
    txn_id = await db.log_wallet_transaction(
        user_discord_id=45678,
        amount_cents=5000,
        balance_after_cents=5000,
        transaction_type="deposit",
        description="Crypto deposit",
        metadata=metadata,
    )
    
    cursor = await db._connection.execute(
        "SELECT * FROM wallet_transactions WHERE id = ?", (txn_id,)
    )
    txn = await cursor.fetchone()
    
    assert txn is not None
    assert txn["metadata"] == metadata
    
    parsed_metadata = json.loads(txn["metadata"])
    assert parsed_metadata["proof"] == "txn_abc123"
    assert parsed_metadata["source"] == "binance"


@pytest.mark.asyncio
async def test_get_orders_for_user(db):
    """Test retrieving orders for a user with pagination."""
    await db.ensure_user(56789)
    await db.update_wallet_balance(56789, 10000)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Basic",
        variant_name="Silver",
        price_cents=500,
    )
    
    order_ids = []
    for i in range(5):
        order_id, _ = await db.purchase_product(
            user_discord_id=56789,
            product_id=product_id,
            price_paid_cents=500,
            discount_applied_percent=0.0,
        )
        order_ids.append(order_id)
    
    orders = await db.get_orders_for_user(56789, limit=10, offset=0)
    assert len(orders) == 5
    
    total = await db.count_orders_for_user(56789)
    assert total == 5
    
    assert orders[0]["id"] == order_ids[-1]


@pytest.mark.asyncio
async def test_get_order_by_id(db):
    """Test retrieving a specific order by ID."""
    await db.ensure_user(67890)
    await db.update_wallet_balance(67890, 1000)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Pro",
        variant_name="Platinum",
        price_cents=800,
    )
    
    order_id, _ = await db.purchase_product(
        user_discord_id=67890,
        product_id=product_id,
        price_paid_cents=800,
        discount_applied_percent=0.0,
        order_metadata=json.dumps({"custom": "field"}),
    )
    
    order = await db.get_order_by_id(order_id)
    assert order is not None
    assert order["id"] == order_id
    assert order["user_discord_id"] == 67890
    assert order["product_id"] == product_id
    assert order["price_paid_cents"] == 800
    
    metadata = json.loads(order["order_metadata"])
    assert metadata["custom"] == "field"


@pytest.mark.asyncio
async def test_get_ticket_by_order_id(db):
    """Test retrieving a ticket associated with an order."""
    await db.ensure_user(78901)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Support",
        variant_name="Basic",
        price_cents=1000,
    )
    
    order_id = await db.create_order(
        user_discord_id=78901,
        product_id=product_id,
        price_paid_cents=1000,
        discount_applied_percent=0.0,
    )
    
    ticket_id = await db.create_ticket(
        user_discord_id=78901,
        channel_id=123456,
        ticket_type="support",
        order_id=order_id,
    )
    
    ticket = await db.get_ticket_by_order_id(order_id)
    assert ticket is not None
    assert ticket["id"] == ticket_id
    assert ticket["order_id"] == order_id
    assert ticket["user_discord_id"] == 78901


@pytest.mark.asyncio
async def test_wallet_transaction_links_to_ticket(db):
    """Test that wallet transactions can link to tickets."""
    await db.ensure_user(89012)
    
    ticket_id = await db.create_ticket(
        user_discord_id=89012,
        channel_id=654321,
        ticket_type="billing",
    )
    
    txn_id = await db.log_wallet_transaction(
        user_discord_id=89012,
        amount_cents=3000,
        balance_after_cents=3000,
        transaction_type="deposit",
        description="Deposit via ticket",
        ticket_id=ticket_id,
    )
    
    cursor = await db._connection.execute(
        "SELECT * FROM wallet_transactions WHERE id = ?", (txn_id,)
    )
    txn = await cursor.fetchone()
    
    assert txn is not None
    assert txn["ticket_id"] == ticket_id


@pytest.mark.asyncio
async def test_orders_pagination(db):
    """Test order pagination with proper ordering."""
    await db.ensure_user(90123)
    await db.update_wallet_balance(90123, 100000)
    
    product_id = await db.create_product(
        main_category="Test",
        sub_category="Service",
        service_name="Test",
        variant_name="Test",
        price_cents=100,
    )
    
    for i in range(25):
        await db.purchase_product(
            user_discord_id=90123,
            product_id=product_id,
            price_paid_cents=100,
            discount_applied_percent=0.0,
        )
    
    page1 = await db.get_orders_for_user(90123, limit=10, offset=0)
    page2 = await db.get_orders_for_user(90123, limit=10, offset=10)
    page3 = await db.get_orders_for_user(90123, limit=10, offset=20)
    
    assert len(page1) == 10
    assert len(page2) == 10
    assert len(page3) == 5
    
    assert page1[0]["id"] > page1[-1]["id"]
    assert page1[-1]["id"] > page2[0]["id"]
