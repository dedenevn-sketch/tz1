"""Миграции создают payments и outbox."""

from sqlalchemy import inspect


async def test_migrations_create_payment_and_outbox_tables(engine):
    async with engine.connect() as conn:
        tables = await conn.run_sync(_schema)

    assert tables["payments"] == {
        "id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "idempotency_key",
        "webhook_url",
        "created_at",
        "processed_at",
        "webhook_sent_at",
    }
    assert tables["outbox"] == {
        "id",
        "aggregate_type",
        "aggregate_id",
        "event_type",
        "payload",
        "created_at",
        "published_at",
    }
    assert tables["idempotency_unique"] is True


def _schema(sync_conn):
    inspector = inspect(sync_conn)
    unique_sets = [
        set(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("payments")
    ]
    unique_sets.extend(
        set(index["column_names"])
        for index in inspector.get_indexes("payments")
        if index["unique"]
    )
    return {
        "payments": {column["name"] for column in inspector.get_columns("payments")},
        "outbox": {column["name"] for column in inspector.get_columns("outbox")},
        "idempotency_unique": {"idempotency_key"} in unique_sets,
    }
