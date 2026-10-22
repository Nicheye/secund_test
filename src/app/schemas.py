import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Self

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, PlainSerializer, field_validator

from app.domain import Currency, InvalidMoney, Money, PaymentStatus
from app.models import Payment

MAX_DESCRIPTION_LENGTH = 1000
MAX_WEBHOOK_URL_LENGTH = 2048

# Amounts go out as strings so that clients never parse money through a float.
AmountString = Annotated[
    Decimal, PlainSerializer(lambda amount: format(amount, "f"), return_type=str)
]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    amount: Decimal
    currency: Currency
    description: str = Field(min_length=1, max_length=MAX_DESCRIPTION_LENGTH)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl = Field(max_length=MAX_WEBHOOK_URL_LENGTH)

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, amount: Decimal) -> Decimal:
        # Money rules do not depend on the currency yet, so any currency validates the amount.
        try:
            Money(amount, Currency.USD)
        except InvalidMoney as error:
            raise ValueError(str(error)) from error
        return amount


class PaymentCreatedResponse(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_payment(cls, payment: Payment) -> Self:
        return cls(
            payment_id=payment.id,
            status=PaymentStatus(payment.status),
            created_at=payment.created_at,
        )


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    amount: AmountString
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: Payment) -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=Currency(payment.currency),
            description=payment.description,
            metadata=payment.payment_metadata,
            status=PaymentStatus(payment.status),
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorBody(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorBody
