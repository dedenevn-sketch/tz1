from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.deps import require_api_key, require_idempotency_key
from app.models import Payment
from app.schemas import PaymentCreate, PaymentCreated, PaymentView
from app.services.payments import create_payment

router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


@router.post("/payments", status_code=202, response_model=PaymentCreated)
async def create(
    body: PaymentCreate,
    idempotency_key: str = Depends(require_idempotency_key),
    session: AsyncSession = Depends(get_session),
) -> PaymentCreated:
    payment = await create_payment(session, body, idempotency_key)
    return PaymentCreated(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/payments/{payment_id}", response_model=PaymentView)
async def get_payment(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentView:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentView.from_payment(payment)
