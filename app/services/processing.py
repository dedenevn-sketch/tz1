import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment
from app.money import format_amount
from app.topology import DLQ, RETRY_QUEUE

Sleep = Callable[[float], Awaitable[None]]


class WebhookDeliveryError(Exception):
    pass


class RandomPaymentGateway:
    def __init__(
        self,
        *,
        success_rate: float,
        delay_min: float,
        delay_max: float,
        rng: random.Random | None = None,
        sleep: Sleep | None = None,
    ):
        self.success_rate = success_rate
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.rng = rng or random.Random()
        self.sleep = sleep or _default_sleep

    async def charge(self, payment) -> bool:
        delay = self.rng.uniform(self.delay_min, self.delay_max)
        await self.sleep(delay)
        return self.rng.random() < self.success_rate


def next_retry_delay(
    failed_attempts: int,
    *,
    max_attempts: int,
    base_seconds: float,
) -> float | None:
    if failed_attempts >= max_attempts:
        return None
    return base_seconds * (2 ** (failed_attempts - 1))


async def handle_payment(
    session: AsyncSession,
    payment_id: UUID,
    gateway,
    webhook,
    *,
    max_attempts: int = 3,
    backoff_base_seconds: float = 1.0,
    sleep: Sleep | None = None,
) -> None:
    pause = sleep or _default_sleep
    payment = await _lock_payment(session, payment_id)
    if payment is None:
        raise LookupError(f"payment {payment_id} not found")
    if payment.webhook_sent_at is not None:
        return

    if payment.status == "pending":
        succeeded = await gateway.charge(payment)
        payment.status = "succeeded" if succeeded else "failed"
        payment.processed_at = datetime.now(timezone.utc)
        await session.commit()
        payment = await _lock_payment(session, payment_id)
        if payment is None or payment.webhook_sent_at is not None:
            return

    try:
        await _deliver_webhook(
            payment,
            webhook,
            max_attempts=max_attempts,
            backoff_base_seconds=backoff_base_seconds,
            sleep=pause,
        )
    except WebhookDeliveryError:
        await session.rollback()
        raise

    payment.webhook_sent_at = datetime.now(timezone.utc)
    await session.commit()


async def process_delivery(
    body: dict,
    headers: dict | None,
    *,
    session_factory,
    gateway,
    webhook,
    broker,
    max_attempts: int = 3,
    backoff_base_seconds: float = 1.0,
) -> None:
    delivery_headers = dict(headers or {})
    try:
        payment_id = UUID(str(body["payment_id"]))
        async with session_factory() as session:
            await handle_payment(session, payment_id, gateway, webhook)
    except Exception:
        failed_attempts = int(delivery_headers.get("x-attempt", 0)) + 1
        delivery_headers["x-attempt"] = failed_attempts
        delay = next_retry_delay(
            failed_attempts,
            max_attempts=max_attempts,
            base_seconds=backoff_base_seconds,
        )
        if delay is None:
            await broker.publish(
                body,
                queue=DLQ,
                headers=delivery_headers,
                expiration=None,
            )
            return
        await broker.publish(
            body,
            queue=RETRY_QUEUE,
            headers=delivery_headers,
            expiration=int(delay * 1000),
        )


async def _lock_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.scalar(
        select(Payment).where(Payment.id == payment_id).with_for_update()
    )


async def _deliver_webhook(
    payment: Payment,
    webhook,
    *,
    max_attempts: int,
    backoff_base_seconds: float,
    sleep: Sleep,
) -> None:
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": format_amount(payment.amount),
        "currency": payment.currency,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            await webhook.send(payment.webhook_url, payload)
            return
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise WebhookDeliveryError(str(exc)) from exc
            await sleep(backoff_base_seconds * (2 ** (attempt - 1)))
    raise WebhookDeliveryError(str(last_error))


async def _default_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
