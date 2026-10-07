"""Consumer: эмуляция шлюза, webhook, повторы и решение об отправке в DLQ."""

from uuid import UUID, uuid4

import pytest

from sqlalchemy import select

from app.models import Payment
from app.services.processing import (
    RandomPaymentGateway,
    WebhookDeliveryError,
    handle_payment,
    next_retry_delay,
    process_delivery,
)
from tests.conftest import auth_headers, payment_body


class FakeGateway:
    def __init__(self, result: bool = True):
        self.result = result
        self.calls = 0

    async def charge(self, payment) -> bool:
        self.calls += 1
        return self.result


class FakeWebhook:
    def __init__(self, fail_times: int = 0):
        self.fail_times = fail_times
        self.calls = []

    async def send(self, url: str, payload: dict) -> None:
        self.calls.append({"url": url, "payload": payload})
        if len(self.calls) <= self.fail_times:
            raise RuntimeError("webhook failed")


class DeliveryBroker:
    def __init__(self):
        self.published = []

    async def publish(self, message, *, queue: str, headers: dict, expiration: int | None):
        self.published.append(
            {
                "message": message,
                "queue": queue,
                "headers": headers,
                "expiration": expiration,
            }
        )


class ScriptedRng:
    def __init__(self, uniforms, rolls):
        self._uniforms = iter(uniforms)
        self._rolls = iter(rolls)

    def uniform(self, start, end):
        return next(self._uniforms)

    def random(self):
        return next(self._rolls)


async def _create(client, key: str) -> str:
    response = await client.post("/api/v1/payments", json=payment_body(), headers=auth_headers(key))
    assert response.status_code == 202
    return response.json()["payment_id"]


async def _load(session_factory, payment_id: str) -> Payment:
    async with session_factory() as session:
        return await session.scalar(select(Payment).where(Payment.id == UUID(payment_id)))


async def test_successful_charge_updates_status_and_sends_webhook(client, session_factory):
    payment_id = await _create(client, "ok-1")
    gateway = FakeGateway(True)
    webhook = FakeWebhook()

    async with session_factory() as session:
        await handle_payment(session, UUID(payment_id), gateway, webhook, sleep=_no_sleep)

    payment = await _load(session_factory, payment_id)
    assert payment.status == "succeeded"
    assert payment.processed_at is not None
    assert payment.processed_at.tzinfo is not None
    assert payment.webhook_sent_at is not None
    assert webhook.calls[0]["url"] == "https://merchant.example/hooks/payment"
    assert webhook.calls[0]["payload"]["payment_id"] == payment_id
    assert webhook.calls[0]["payload"]["status"] == "succeeded"
    assert webhook.calls[0]["payload"]["amount"] == "10.50"
    assert webhook.calls[0]["payload"]["currency"] == "RUB"
    assert gateway.calls == 1


async def test_declined_charge_is_a_completed_failed_payment(client, session_factory):
    payment_id = await _create(client, "decline-1")
    gateway = FakeGateway(False)
    webhook = FakeWebhook()
    broker = DeliveryBroker()

    await process_delivery(
        {"payment_id": payment_id},
        {},
        session_factory=session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
    )

    payment = await _load(session_factory, payment_id)
    assert payment.status == "failed"
    assert payment.webhook_sent_at is not None
    assert webhook.calls[0]["payload"]["status"] == "failed"
    assert broker.published == []
    assert gateway.calls == 1


async def test_second_delivery_does_not_charge_or_notify_again(client, session_factory):
    payment_id = await _create(client, "twice-1")
    gateway = FakeGateway(True)
    webhook = FakeWebhook()

    for _ in range(2):
        async with session_factory() as session:
            await handle_payment(session, UUID(payment_id), gateway, webhook, sleep=_no_sleep)

    assert gateway.calls == 1
    assert len(webhook.calls) == 1


async def test_webhook_is_retried_with_exponential_delay(client, session_factory):
    payment_id = await _create(client, "hook-retry")
    webhook = FakeWebhook(fail_times=2)
    sleeps = []

    async def record(seconds):
        sleeps.append(seconds)

    async with session_factory() as session:
        await handle_payment(
            session,
            UUID(payment_id),
            FakeGateway(True),
            webhook,
            max_attempts=3,
            backoff_base_seconds=0.5,
            sleep=record,
        )

    assert len(webhook.calls) == 3
    assert sleeps == [0.5, 1.0]
    payment = await _load(session_factory, payment_id)
    assert payment.webhook_sent_at is not None


async def test_exhausted_webhook_can_be_redelivered_without_a_second_charge(client, session_factory):
    payment_id = await _create(client, "hook-fail")
    gateway = FakeGateway(True)
    webhook = FakeWebhook(fail_times=5)

    async with session_factory() as session:
        with pytest.raises(WebhookDeliveryError):
            await handle_payment(
                session,
                UUID(payment_id),
                gateway,
                webhook,
                max_attempts=3,
                backoff_base_seconds=0.25,
                sleep=_no_sleep,
            )

    assert gateway.calls == 1
    assert len(webhook.calls) == 3
    payment = await _load(session_factory, payment_id)
    assert payment.status == "succeeded"
    assert payment.webhook_sent_at is None

    recovered = FakeWebhook()
    async with session_factory() as session:
        await handle_payment(
            session,
            UUID(payment_id),
            gateway,
            recovered,
            max_attempts=3,
            backoff_base_seconds=1,
            sleep=_no_sleep,
        )

    assert gateway.calls == 1
    assert len(recovered.calls) == 1
    assert recovered.calls[0]["payload"]["status"] == "succeeded"
    payment = await _load(session_factory, payment_id)
    assert payment.webhook_sent_at is not None


def test_retry_delay_doubles_until_attempt_limit():
    assert next_retry_delay(1, max_attempts=3, base_seconds=1) == 1
    assert next_retry_delay(2, max_attempts=3, base_seconds=1) == 2
    assert next_retry_delay(3, max_attempts=3, base_seconds=1) is None
    assert next_retry_delay(1, max_attempts=3, base_seconds=0.5) == 0.5
    assert next_retry_delay(2, max_attempts=3, base_seconds=0.5) == 1


async def test_first_handler_failure_is_scheduled_on_retry_queue(session_factory):
    broker = DeliveryBroker()
    body = {"payment_id": str(uuid4())}

    await process_delivery(
        body,
        {},
        session_factory=session_factory,
        gateway=FakeGateway(),
        webhook=FakeWebhook(),
        broker=broker,
        max_attempts=3,
        backoff_base_seconds=1,
    )

    assert broker.published == [
        {
            "message": body,
            "queue": "payments.new.retry",
            "headers": {"x-attempt": 1},
            "expiration": 1000,
        }
    ]


async def test_second_failure_keeps_headers_and_doubles_delay(session_factory):
    broker = DeliveryBroker()
    body = {"payment_id": str(uuid4())}

    await process_delivery(
        body,
        {"x-attempt": 1, "trace": "abc"},
        session_factory=session_factory,
        gateway=FakeGateway(),
        webhook=FakeWebhook(),
        broker=broker,
        max_attempts=3,
        backoff_base_seconds=1,
    )

    published = broker.published[0]
    assert published["queue"] == "payments.new.retry"
    assert published["expiration"] == 2000
    assert published["headers"]["x-attempt"] == 2
    assert published["headers"]["trace"] == "abc"
    assert published["message"] == body


async def test_third_failure_is_sent_to_dlq(session_factory):
    broker = DeliveryBroker()
    body = {"payment_id": str(uuid4())}

    await process_delivery(
        body,
        {"x-attempt": 2},
        session_factory=session_factory,
        gateway=FakeGateway(),
        webhook=FakeWebhook(),
        broker=broker,
        max_attempts=3,
        backoff_base_seconds=1,
    )

    assert broker.published == [
        {
            "message": body,
            "queue": "payments.new.dlq",
            "headers": {"x-attempt": 3},
            "expiration": None,
        }
    ]


async def test_successful_delivery_is_not_republished(client, session_factory):
    payment_id = await _create(client, "deliver-ok")
    broker = DeliveryBroker()

    await process_delivery(
        {"payment_id": payment_id},
        {},
        session_factory=session_factory,
        gateway=FakeGateway(True),
        webhook=FakeWebhook(),
        broker=broker,
    )

    assert broker.published == []
    payment = await _load(session_factory, payment_id)
    assert payment.status == "succeeded"


async def test_gateway_waits_between_two_and_five_seconds_and_uses_success_rate():
    sleeps = []

    async def record(seconds):
        sleeps.append(seconds)

    success = RandomPaymentGateway(
        success_rate=0.9,
        delay_min=2,
        delay_max=5,
        rng=ScriptedRng([3.5], [0.1]),
        sleep=record,
    )
    decline = RandomPaymentGateway(
        success_rate=0.9,
        delay_min=2,
        delay_max=5,
        rng=ScriptedRng([2], [0.95]),
        sleep=record,
    )

    assert await success.charge(None) is True
    assert await decline.charge(None) is False
    assert sleeps == [3.5, 2]


async def _no_sleep(_seconds):
    return None
