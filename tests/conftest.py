import pytest
import pytest_asyncio
from apex_core.database import Database


@pytest_asyncio.fixture
async def db():
    database = Database(":memory:")
    await database.connect()

    await database._connection.execute(
        """
        INSERT INTO products (
            id, main_category, sub_category, service_name, variant_name, price_cents
        ) VALUES (0, 'Manual', 'Manual', 'Manual', 'Manual Placeholder', 0)
        ON CONFLICT(id) DO NOTHING
        """
    )
    await database._connection.commit()

    yield database
    await database.close()
