from decimal import Decimal

import pytest

from app.domain import Currency, InvalidMoney, Money


@pytest.mark.parametrize(
    "amount",
    [Decimal("0.01"), Decimal("1"), Decimal("10.50"), Decimal("1.500"), Decimal("1000000000.00")],
)
def test_accepts_valid_amount(amount: Decimal) -> None:
    money = Money(amount, Currency.USD)

    assert money.amount == amount
    assert money.currency is Currency.USD


@pytest.mark.parametrize(
    "amount",
    [
        Decimal("0"),
        Decimal("-0.01"),
        Decimal("-100"),
        Decimal("0.001"),
        Decimal("10.505"),
        Decimal("1000000000.01"),
        Decimal("NaN"),
        Decimal("Infinity"),
    ],
)
def test_rejects_invalid_decimal_amount(amount: Decimal) -> None:
    with pytest.raises(InvalidMoney):
        Money(amount, Currency.RUB)


@pytest.mark.parametrize("amount", [10.5, 1, "10.50", None])
def test_rejects_non_decimal_amount(amount: object) -> None:
    with pytest.raises(InvalidMoney):
        Money(amount, Currency.EUR)  # type: ignore[arg-type]


def test_money_is_immutable() -> None:
    money = Money(Decimal("5.00"), Currency.EUR)

    with pytest.raises(AttributeError):
        money.amount = Decimal("6.00")  # type: ignore[misc]


def test_money_with_different_currency_is_not_equal() -> None:
    assert Money(Decimal("5.00"), Currency.EUR) != Money(Decimal("5.00"), Currency.USD)


@pytest.mark.parametrize("currency", [Currency.RUB, Currency.USD, Currency.EUR])
def test_accepts_every_supported_currency(currency: Currency) -> None:
    assert Money(Decimal("1.00"), currency).currency is currency


def test_supported_currencies_are_exactly_rub_usd_eur() -> None:
    assert [currency.value for currency in Currency] == ["RUB", "USD", "EUR"]


@pytest.mark.parametrize(
    ("amount", "message"),
    [
        (Decimal("0"), "Amount must be positive"),
        (Decimal("-1.00"), "Amount must be positive"),
        (Decimal("0.001"), "Amount must have at most 2 decimal places"),
        (Decimal("1000000000.01"), "Amount must not exceed 1000000000.00"),
        (Decimal("1E+400"), "Amount must not exceed 1000000000.00"),
        (Decimal("1E+30"), "Amount must not exceed 1000000000.00"),
        (Decimal("1000000000.001"), "Amount must not exceed 1000000000.00"),
        (Decimal("1E-400"), "Amount must have at most 2 decimal places"),
        (Decimal("NaN"), "Amount must be a finite number"),
        (Decimal("-Infinity"), "Amount must be a finite number"),
    ],
)
def test_rejects_invalid_amount_with_message_naming_the_violated_rule(
    amount: Decimal, message: str
) -> None:
    with pytest.raises(InvalidMoney) as error:
        Money(amount, Currency.USD)

    assert str(error.value) == message


def test_rejects_float_amount_with_message_naming_the_type() -> None:
    with pytest.raises(InvalidMoney) as error:
        Money(10.5, Currency.USD)  # type: ignore[arg-type]

    assert str(error.value) == "Amount must be a Decimal, got float"


def test_accepts_amount_with_trailing_zeros_beyond_two_places() -> None:
    assert Money(Decimal("1.5000"), Currency.USD).amount == Decimal("1.50")
