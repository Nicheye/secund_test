import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Self

from pydantic import AfterValidator, AnyHttpUrl, BaseModel, ConfigDict, Field, PlainSerializer

from app.domain import Currency, PaymentStatus, validate_amount
from app.models import Payment

MAX_DESCRIPTION_LENGTH = 1000
MAX_WEBHOOK_URL_LENGTH = 2048
MAX_METADATA_BYTES = 16 * 1024

# Amounts go out as strings so that clients never parse money through a float.
AmountString = Annotated[
    Decimal, PlainSerializer(lambda amount: format(amount, "f"), return_type=str)
]


def _validate_metadata_size(metadata: dict[str, Any]) -> dict[str, Any]:
    # Compact UTF-8 JSON, so the limit does not depend on the client's whitespace.
    size = len(json.dumps(metadata, separators=(",", ":"), ensure_ascii=False).encode())
    if size > MAX_METADATA_BYTES:
        raise ValueError(f"Metadata must be at most {MAX_METADATA_BYTES} bytes as compact JSON")
    return metadata


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    amount: Annotated[Decimal, AfterValidator(validate_amount)]
    currency: Currency
    description: str = Field(default="", max_length=MAX_DESCRIPTION_LENGTH)
    metadata: Annotated[dict[str, Any], AfterValidator(_validate_metadata_size)] = Field(
        default_factory=dict
    )
    webhook_url: AnyHttpUrl = Field(max_length=MAX_WEBHOOK_URL_LENGTH)


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
