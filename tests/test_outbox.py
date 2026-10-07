"""Outbox: событие пишется в той же транзакции и публикуется после коммита."""

import pytest
from sqlalchemy import select

from app.models import Outbox
from app.services.outbox import publish_pending
from tests.conftest import auth_headers, payment_body


class OutboxBroker:
    def __init__(self):
        self.published = []

    async def publish(self, message, *, exchange: str, routing_key: str):
        self.published.append(
            {"message": message, "exchange": exchange, "routing_key": routing_key}
        )


class FailingBroker(OutboxBroker):
    async def publish(self, message, *, exchange: str, routing_key: str):
        raise ConnectionError("broker down")


async def test_create_writes_unpublished_outbox_event(client, session_factory):
    created = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers=auth_headers("outbox-1"),
    )
    payment_id = created.json()["payment_id"]

    async with session_factory() as session:
        event = await session.scalar(select(Outbox))

    assert event is not None
    assert event.aggregate_type == "payment"
    assert str(event.aggregate_id) == payment_id
    assert event.event_type == "payments.new"
    assert event.payload["payment_id"] == payment_id
    assert event.published_at is None


async def test_publisher_marks_events_and_skips_them_next_time(client, session_factory):
    await client.post("/api/v1/payments", json=payment_body(), headers=auth_headers("pub-1"))
    await client.post(
        "/api/v1/payments",
        json=payment_body(amount="3.00"),
        headers=auth_headers("pub-2"),
    )
    broker = OutboxBroker()

    async with session_factory() as session:
        published = await publish_pending(session, broker)

    assert published == 2
    assert len(broker.published) == 2
    assert {item["exchange"] for item in broker.published} == {"payments"}
    assert {item["routing_key"] for item in broker.published} == {"payments.new"}

    async with session_factory() as session:
        events = (await session.scalars(select(Outbox))).all()
        assert len(events) == 2
        assert all(event.published_at is not None for event in events)
        published_again = await publish_pending(session, broker)

    assert published_again == 0
    assert len(broker.published) == 2


async def test_publisher_leaves_event_unpublished_when_broker_fails(client, session_factory):
    await client.post("/api/v1/payments", json=payment_body(), headers=auth_headers("pub-fail"))

    async with session_factory() as session:
        with pytest.raises(ConnectionError):
            await publish_pending(session, FailingBroker())

    async with session_factory() as session:
        event = await session.scalar(select(Outbox))
    assert event.published_at is None
