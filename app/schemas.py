from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

from app.money import TWOPLACES, format_amount


class Currency(str, Enum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0)
    currency: Currency
    description: str = Field(min_length=1, max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, value: object) -> object:
        if isinstance(value, float):
            return Decimal(str(value))
        return value

    @field_validator("amount")
    @classmethod
    def two_decimal_places(cls, value: Decimal) -> Decimal:
        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -2:
            raise ValueError("amount must have at most 2 decimal places")
        return value.quantize(TWOPLACES)


class PaymentCreated(BaseModel):
    payment_id: UUID
    status: str
    created_at: datetime


class PaymentView(BaseModel):
    payment_id: UUID
    amount: str
    currency: str
    description: str
    metadata: dict[str, Any]
    status: str
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment) -> "PaymentView":
        return cls(
            payment_id=payment.id,
            amount=format_amount(payment.amount),
            currency=payment.currency,
            description=payment.description,
            metadata=payment.payment_metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
