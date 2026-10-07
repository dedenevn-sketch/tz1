import os

os.environ["DATABASE_URL"] = "postgresql+asyncpg://payments:payments@127.0.0.1:5432/payments_test"
os.environ["RABBITMQ_URL"] = "amqp://guest:guest@127.0.0.1:5672/"
os.environ["API_KEY"] = "test-api-key"
os.environ["OUTBOX_RELAY_ENABLED"] = "false"
os.environ["PROCESS_DELAY_MIN_SECONDS"] = "0"
os.environ["PROCESS_DELAY_MAX_SECONDS"] = "0"
os.environ["PROCESS_SUCCESS_RATE"] = "1"

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings
from app.db import get_session
from app.main import create_app

API_KEY = "test-api-key"


def auth_headers(idempotency_key: str = "idem-1", api_key: str = API_KEY) -> dict[str, str]:
    return {"X-API-Key": api_key, "Idempotency-Key": idempotency_key}


def payment_body(**overrides) -> dict:
    body = {
        "amount": "10.50",
        "currency": "RUB",
        "description": "Заказ 15",
        "metadata": {"order_id": "15"},
        "webhook_url": "https://merchant.example/hooks/payment",
    }
    body.update(overrides)
    return body


def _upgrade_schema() -> None:
    get_settings.cache_clear()
    command.upgrade(Config("alembic.ini"), "head")


async def _truncate(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE payments, outbox RESTART IDENTITY CASCADE"))


@pytest.fixture(scope="session", loop_scope="session")
async def engine():
    await asyncio.to_thread(_upgrade_schema)
    eng = create_async_engine(os.environ["DATABASE_URL"])
    yield eng
    await eng.dispose()


@pytest.fixture(scope="session", loop_scope="session")
async def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture(autouse=True, loop_scope="session")
async def clean_tables(engine):
    await _truncate(engine)
    yield
    await _truncate(engine)


@pytest.fixture(loop_scope="session")
async def client(session_factory):
    app = create_app()

    async def override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http
