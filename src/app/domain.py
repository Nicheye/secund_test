from dataclasses import dataclass
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


@dataclass(frozen=True)
class Money:
    """Amount is exact: values with more than two decimal places are rejected, never rounded."""

    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise InvalidMoney(f"Amount must be a Decimal, got {type(self.amount).__name__}")
        if not self.amount.is_finite():
            raise InvalidMoney("Amount must be a finite number")
        if self.amount <= 0:
            raise InvalidMoney("Amount must be positive")
        # The range is checked before quantize(), which raises InvalidOperation on huge values.
        if self.amount > MAX_AMOUNT:
            raise InvalidMoney(f"Amount must not exceed {MAX_AMOUNT}")
        if self.amount != self.amount.quantize(AMOUNT_SCALE):
            raise InvalidMoney("Amount must have at most 2 decimal places")
