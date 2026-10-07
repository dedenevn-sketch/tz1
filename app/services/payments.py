from datetime import datetime, timezone
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox, Payment
from app.money import format_amount
from app.schemas import PaymentCreate


def _same_request(payment: Payment, data: PaymentCreate) -> bool:
    return (
        payment.amount == data.amount
        and payment.currency == data.currency.value
        and payment.description == data.description
        and payment.payment_metadata == data.metadata
        and payment.webhook_url == str(data.webhook_url)
    )


async def _find_by_key(session: AsyncSession, idempotency_key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))


async def create_payment(
    session: AsyncSession,
    data: PaymentCreate,
    idempotency_key: str,
) -> Payment:
    existing = await _find_by_key(session, idempotency_key)
    if existing is not None:
        return _replay_or_conflict(existing, data)

    now = datetime.now(timezone.utc)
    payment = Payment(
        id=uuid4(),
        amount=data.amount,
        currency=data.currency.value,
        description=data.description,
        payment_metadata=data.metadata,
        status="pending",
        idempotency_key=idempotency_key,
        webhook_url=str(data.webhook_url),
        created_at=now,
    )
    session.add(payment)
    session.add(
        Outbox(
            id=uuid4(),
            aggregate_type="payment",
            aggregate_id=payment.id,
            event_type="payments.new",
            payload={
                "payment_id": str(payment.id),
                "amount": format_amount(payment.amount),
                "currency": payment.currency,
            },
            created_at=now,
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await _find_by_key(session, idempotency_key)
        if existing is None:
            raise
        return _replay_or_conflict(existing, data)
    return payment


def _replay_or_conflict(payment: Payment, data: PaymentCreate) -> Payment:
    if _same_request(payment, data):
        return payment
    raise HTTPException(
        status_code=409,
        detail="Idempotency key already used with a different payload",
    )
