import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

AMOUNT_SCALE = Decimal("0.01")
MAX_AMOUNT = Decimal("1000000000.00")


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class IllegalStatusTransition(Exception):
    def __init__(self, current: PaymentStatus, target: PaymentStatus) -> None:
        super().__init__(f"Illegal payment status transition: {current} -> {target}")
        self.current = current
        self.target = target


class InvalidMoney(ValueError):
    pass


ALLOWED_TRANSITIONS: dict[PaymentStatus, frozenset[PaymentStatus]] = {
    PaymentStatus.PENDING: frozenset({PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}),
    PaymentStatus.SUCCEEDED: frozenset(),
    PaymentStatus.FAILED: frozenset(),
}


def transition(current: PaymentStatus, target: PaymentStatus) -> PaymentStatus:
    if target not in ALLOWED_TRANSITIONS[current]:
        raise IllegalStatusTransition(current, target)
    return target


def validate_amount(amount: Decimal) -> Decimal:
    """Returns the amount if it is a valid payment amount, otherwise raises InvalidMoney.

    Exact: values with more than two decimal places are rejected, never rounded. The rules do
    not depend on the currency yet.
    """
    if not isinstance(amount, Decimal):
        raise InvalidMoney(f"Amount must be a Decimal, got {type(amount).__name__}")
    if not amount.is_finite():
        raise InvalidMoney("Amount must be a finite number")
    if amount <= 0:
        raise InvalidMoney("Amount must be positive")
    # The range is checked before quantize(), which raises InvalidOperation on huge values.
    if amount > MAX_AMOUNT:
        raise InvalidMoney(f"Amount must not exceed {MAX_AMOUNT}")
    if amount != amount.quantize(AMOUNT_SCALE):
        raise InvalidMoney("Amount must have at most 2 decimal places")
    return amount


@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        validate_amount(self.amount)


@dataclass(frozen=True)
class PaymentSnapshot:
    """What the payment gateway and the webhook sender need to know about a payment."""

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    webhook_url: str
    processed_at: datetime | None
