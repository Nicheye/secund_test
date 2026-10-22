from typing import Any

import pytest

from app.schemas import CreatePaymentRequest
from app.services import compute_request_fingerprint

BASE: dict[str, Any] = {
    "amount": "100.50",
    "currency": "USD",
    "description": "Order 42",
    "metadata": {"a": 1, "b": {"c": [1, 2]}},
    "webhook_url": "https://merchant.example.com/hook",
}


def fingerprint(**overrides: Any) -> str:
    return compute_request_fingerprint(CreatePaymentRequest.model_validate({**BASE, **overrides}))


def test_fingerprint_is_a_sha256_hex_digest() -> None:
    assert len(fingerprint()) == 64
    assert set(fingerprint()) <= set("0123456789abcdef")


def test_fingerprint_is_stable_for_identical_payloads() -> None:
    assert fingerprint() == fingerprint()


def test_fingerprint_ignores_metadata_key_order_at_every_level() -> None:
    reordered = {"b": {"c": [1, 2]}, "a": 1}

    assert fingerprint(metadata=reordered) == fingerprint()


def test_fingerprint_respects_metadata_list_order() -> None:
    assert fingerprint(metadata={"a": 1, "b": {"c": [2, 1]}}) != fingerprint()


@pytest.mark.parametrize("amount", ["100.5", "100.50", "100.500", 100.5, "1.005e2"])
def test_fingerprint_treats_numerically_equal_amounts_as_the_same(amount: Any) -> None:
    assert fingerprint(amount=amount) == fingerprint()


@pytest.mark.parametrize("amount", ["1e2", "100", "100.00", 100])
def test_fingerprint_treats_whole_amounts_written_differently_as_the_same(amount: Any) -> None:
    assert fingerprint(amount=amount) == fingerprint(amount="100.0")


@pytest.mark.parametrize(
    "changed",
    [
        {"amount": "100.51"},
        {"currency": "EUR"},
        {"description": "Order 43"},
        {"metadata": {}},
        {"metadata": {"a": 2, "b": {"c": [1, 2]}}},
        {"webhook_url": "https://merchant.example.com/other"},
    ],
)
def test_fingerprint_changes_when_any_single_field_changes(changed: dict[str, Any]) -> None:
    assert fingerprint(**changed) != fingerprint()


def test_fingerprint_distinguishes_string_and_number_metadata_values() -> None:
    assert fingerprint(metadata={"a": "1"}) != fingerprint(metadata={"a": 1})
