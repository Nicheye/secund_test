import json
import uuid
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, Payment

PAYMENTS_URL = "/api/v1/payments"
VALID_BODY: dict[str, Any] = {
    "amount": "100.50",
    "currency": "USD",
    "description": "Order 42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://merchant.example.com/hooks/payments",
}


def headers(
    api_key: str | None = "test-key", idempotency_key: str | None = "key-1"
) -> dict[str, str]:
    result = {}
    if api_key is not None:
        result["X-API-Key"] = api_key
    if idempotency_key is not None:
        result["Idempotency-Key"] = idempotency_key
    return result


async def count_rows(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model)) or 0


@pytest.mark.parametrize("api_key", [None, "", "wrong-key", "test-key-extra", "TEST-KEY"])
async def test_rejects_create_with_401_when_api_key_is_missing_or_wrong(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    api_key: str | None,
) -> None:
    response = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers(api_key=api_key))

    assert response.status_code == 401
    assert response.json() == {
        "error": {"code": "unauthorized", "message": "Missing or invalid API key"}
    }
    assert await count_rows(session_factory, Payment) == 0


async def test_rejects_create_with_400_when_idempotency_key_is_missing(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(
        PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key=None)
    )

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "code": "idempotency_key_required",
            "message": "Idempotency-Key header is required",
        }
    }
    assert await count_rows(session_factory, Payment) == 0


async def test_rejects_create_with_400_when_idempotency_key_is_longer_than_255_characters(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(
        PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key="k" * 256)
    )

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "code": "idempotency_key_invalid",
            "message": "Idempotency-Key must be at most 255 characters",
        }
    }
    assert await count_rows(session_factory, Payment) == 0


async def test_accepts_create_when_idempotency_key_is_exactly_255_characters(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(
        PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key="k" * 255)
    )

    assert response.status_code == 202
    assert await count_rows(session_factory, Payment) == 1


async def test_stores_neither_payment_nor_outbox_event_when_outbox_insert_fails(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    with (
        patch(
            "app.repositories.insert_outbox_event", side_effect=RuntimeError("outbox unavailable")
        ),
        pytest.raises(RuntimeError, match="outbox unavailable"),
    ):
        await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    assert await count_rows(session_factory, Payment) == 0
    assert await count_rows(session_factory, OutboxEvent) == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "0.00"),
        ("amount", "-10.00"),
        ("amount", "10.005"),
        ("amount", 10.005),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("amount", "not-a-number"),
        ("amount", "1000000000.01"),
        ("amount", None),
        ("currency", "GBP"),
        ("currency", "usd"),
        ("currency", ""),
        ("webhook_url", "ftp://merchant.example.com/hook"),
        ("webhook_url", "javascript:alert(1)"),
        ("webhook_url", "not a url"),
        ("webhook_url", ""),
    ],
)
async def test_rejects_create_with_422_when_field_is_invalid(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    field: str,
    value: Any,
) -> None:
    body = {**VALID_BODY, field: value}

    response = await client.post(PAYMENTS_URL, json=body, headers=headers())

    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "validation_error"
    assert set(payload) == {"error"}
    assert await count_rows(session_factory, Payment) == 0
    assert await count_rows(session_factory, OutboxEvent) == 0


@pytest.mark.parametrize("missing_field", ["amount", "currency", "webhook_url"])
async def test_rejects_create_with_422_when_required_field_is_missing(
    client: httpx.AsyncClient, missing_field: str
) -> None:
    body = {key: value for key, value in VALID_BODY.items() if key != missing_field}

    response = await client.post(PAYMENTS_URL, json=body, headers=headers())

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_accepts_create_and_stores_pending_payment_with_outbox_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    assert response.status_code == 202
    created = response.json()
    assert set(created) == {"payment_id", "status", "created_at"}
    assert created["status"] == "pending"
    async with session_factory() as session:
        payments = (await session.scalars(select(Payment))).all()
        events = (await session.scalars(select(OutboxEvent))).all()
    assert len(payments) == 1
    payment = payments[0]
    assert str(payment.id) == created["payment_id"]
    assert str(payment.amount) == "100.50"
    assert payment.currency == "USD"
    assert payment.description == "Order 42"
    assert payment.payment_metadata == {"order_id": "42"}
    assert payment.status == "pending"
    assert payment.idempotency_key == "key-1"
    assert payment.webhook_url == "https://merchant.example.com/hooks/payments"
    assert payment.processed_at is None
    assert len(events) == 1
    assert events[0].event_type == "payment.created"
    assert events[0].aggregate_id == payment.id
    assert events[0].payload == {"payment_id": created["payment_id"]}
    assert events[0].published_at is None


async def test_defaults_metadata_to_empty_object_when_omitted(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    body = {key: value for key, value in VALID_BODY.items() if key != "metadata"}

    response = await client.post(PAYMENTS_URL, json=body, headers=headers())

    assert response.status_code == 202
    async with session_factory() as session:
        payment = await session.scalar(select(Payment))
    assert payment is not None
    assert payment.payment_metadata == {}


async def test_returns_original_payment_when_same_key_and_same_payload_are_repeated(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    second = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count_rows(session_factory, Payment) == 1
    assert await count_rows(session_factory, OutboxEvent) == 1


async def test_treats_equal_decimal_amounts_as_same_payload(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": "100.50"}, headers=headers()
    )

    second = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": "100.5"}, headers=headers()
    )

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count_rows(session_factory, Payment) == 1


@pytest.mark.parametrize(
    "changed",
    [
        {"amount": "100.51"},
        {"currency": "EUR"},
        {"description": "Order 43"},
        {"metadata": {"order_id": "43"}},
        {"webhook_url": "https://merchant.example.com/other"},
    ],
)
async def test_rejects_create_with_409_when_same_key_is_used_with_different_payload(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    changed: dict[str, Any],
) -> None:
    first = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    second = await client.post(PAYMENTS_URL, json={**VALID_BODY, **changed}, headers=headers())

    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json() == {
        "error": {
            "code": "idempotency_key_conflict",
            "message": "Idempotency-Key was already used with a different request payload",
        }
    }
    assert await count_rows(session_factory, Payment) == 1
    assert await count_rows(session_factory, OutboxEvent) == 1


async def test_creates_separate_payments_for_different_idempotency_keys(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key="a"))

    second = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key="b"))

    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await count_rows(session_factory, Payment) == 2
    assert await count_rows(session_factory, OutboxEvent) == 2


@pytest.mark.parametrize(
    ("amount", "message"),
    [
        ("0", "body.amount: Value error, Amount must be positive"),
        ("-5.00", "body.amount: Value error, Amount must be positive"),
        ("1.005", "body.amount: Value error, Amount must have at most 2 decimal places"),
    ],
)
async def test_422_message_names_the_amount_field_and_the_violated_rule(
    client: httpx.AsyncClient, amount: str, message: str
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": amount}, headers=headers()
    )

    assert response.status_code == 422
    assert response.json() == {"error": {"code": "validation_error", "message": message}}


async def fetch_payments(session_factory: async_sessionmaker[AsyncSession]) -> list[Payment]:
    async with session_factory() as session:
        return list((await session.scalars(select(Payment))).all())


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
async def test_accepts_create_for_every_supported_currency(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], currency: str
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "currency": currency}, headers=headers()
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.currency == currency


async def test_returns_202_body_with_exactly_payment_id_pending_status_and_created_at(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    [payment] = await fetch_payments(session_factory)
    assert response.status_code == 202
    assert response.json() == {
        "payment_id": str(payment.id),
        "status": "pending",
        "created_at": payment.created_at.isoformat().replace("+00:00", "Z"),
    }


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"order_id": "42"},
        {"nested": {"list": [1, 2, {"deep": None}], "flag": True}, "number": 1.5},
        {"unicode": "Заказ №42 ✓"},
        {"": "empty key"},
    ],
)
async def test_stores_metadata_exactly_as_sent(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    metadata: dict[str, Any],
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "metadata": metadata}, headers=headers()
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.payment_metadata == metadata


@pytest.mark.parametrize("metadata", [None, [], "text", 5])
async def test_rejects_create_with_422_when_metadata_is_not_an_object(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    metadata: Any,
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "metadata": metadata}, headers=headers()
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await fetch_payments(session_factory) == []


METADATA_LIMIT_BYTES = 16384
# `{"k":""}` is 8 bytes of compact JSON, so the filler brings the total to the target size.
METADATA_FRAME_BYTES = 8


@pytest.mark.parametrize(
    "filler",
    [
        "x" * (METADATA_LIMIT_BYTES - METADATA_FRAME_BYTES),
        # Two bytes per character in UTF-8: the limit counts bytes, not characters.
        "ж" * ((METADATA_LIMIT_BYTES - METADATA_FRAME_BYTES) // 2),
    ],
)
async def test_accepts_metadata_of_exactly_sixteen_kibibytes(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], filler: str
) -> None:
    metadata = {"k": filler}

    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "metadata": metadata}, headers=headers()
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.payment_metadata == metadata


@pytest.mark.parametrize(
    "filler",
    [
        "x" * (METADATA_LIMIT_BYTES - METADATA_FRAME_BYTES + 1),
        "ж" * ((METADATA_LIMIT_BYTES - METADATA_FRAME_BYTES) // 2 + 1),
    ],
)
async def test_rejects_metadata_over_sixteen_kibibytes_with_422_naming_metadata(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], filler: str
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "metadata": {"k": filler}}, headers=headers()
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": (
                "body.metadata: Value error, Metadata must be at most 16384 bytes as compact JSON"
            ),
        }
    }
    assert await fetch_payments(session_factory) == []


@pytest.mark.parametrize("description", ["", "x", "Order 42", "d" * 1000, "Заказ", "line\nbreak"])
async def test_accepts_description_from_zero_to_one_thousand_characters(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], description: str
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "description": description}, headers=headers()
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.description == description


async def test_stores_and_returns_an_empty_description_when_it_is_omitted(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    body = {key: value for key, value in VALID_BODY.items() if key != "description"}

    response = await client.post(PAYMENTS_URL, json=body, headers=headers())

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.description == ""
    fetched = await client.get(f"{PAYMENTS_URL}/{response.json()['payment_id']}", headers=headers())
    assert fetched.json()["description"] == ""


async def test_omitted_and_empty_description_are_the_same_payload_for_idempotency(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    omitted = {key: value for key, value in VALID_BODY.items() if key != "description"}

    first = await client.post(PAYMENTS_URL, json=omitted, headers=headers())
    second = await client.post(PAYMENTS_URL, json={**omitted, "description": ""}, headers=headers())

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count_rows(session_factory, Payment) == 1


@pytest.mark.parametrize("description", ["d" * 1001, 42, None, ["a"]])
async def test_rejects_create_with_422_when_description_is_too_long_or_not_text(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    description: Any,
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "description": description}, headers=headers()
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert "description" in response.json()["error"]["message"]
    assert await fetch_payments(session_factory) == []


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        ("0.01", "0.01"),
        ("0.1", "0.10"),
        ("1", "1.00"),
        (100, "100.00"),
        (100.5, "100.50"),
        ("100.50", "100.50"),
        ("1e2", "100.00"),
        ("999999999.99", "999999999.99"),
        ("1000000000", "1000000000.00"),
        ("1000000000.00", "1000000000.00"),
    ],
)
async def test_accepts_amounts_inside_the_allowed_range_and_stores_them_with_two_decimals(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    sent: Any,
    stored: str,
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": sent}, headers=headers()
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert str(payment.amount) == stored


@pytest.mark.parametrize(
    "amount",
    [
        "0.001",
        "0.009",
        "-0.01",
        "-Infinity",
        "inf",
        "sNaN",
        "1e400",
        "1,5",
        "",
        " ",
        "10 USD",
        True,
        [],
        {},
        0.30000000000000004,
        1000000000.01,
    ],
)
async def test_rejects_create_with_422_when_amount_is_out_of_range_or_malformed(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    amount: Any,
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": amount}, headers=headers()
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert "amount" in response.json()["error"]["message"]
    assert await fetch_payments(session_factory) == []


@pytest.mark.parametrize("currency", [None, 1, "RUR", "EUR ", "Usd", ["USD"]])
async def test_rejects_create_with_422_when_currency_is_unknown_or_not_text(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    currency: Any,
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "currency": currency}, headers=headers()
    )

    assert response.status_code == 422
    assert "currency" in response.json()["error"]["message"]
    assert await fetch_payments(session_factory) == []


@pytest.mark.parametrize(
    "webhook_url",
    [
        "http://merchant.example.com/hook",
        "https://merchant.example.com:8443/hook?token=abc&x=1",
        "https://merchant.example.com",
        "https://203.0.113.7/hook",
    ],
)
async def test_accepts_http_and_https_webhook_urls(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], webhook_url: str
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "webhook_url": webhook_url}, headers=headers()
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.webhook_url.startswith(webhook_url.rstrip("/"))


@pytest.mark.parametrize(
    "webhook_url",
    [
        "ftp://merchant.example.com/hook",
        "merchant.example.com/hook",
        "//merchant.example.com/hook",
        "https://",
        "file:///etc/passwd",
        "https://merchant.example.com/" + "a" * 2048,
        None,
        42,
        ["https://merchant.example.com/hook"],
    ],
)
async def test_rejects_create_with_422_when_webhook_url_is_not_a_short_http_url(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    webhook_url: Any,
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "webhook_url": webhook_url}, headers=headers()
    )

    assert response.status_code == 422
    assert "webhook_url" in response.json()["error"]["message"]
    assert await fetch_payments(session_factory) == []


async def test_rejects_create_with_422_when_body_has_an_unknown_field(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "status": "succeeded"}, headers=headers()
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": "body.status: Extra inputs are not permitted",
        }
    }
    assert await fetch_payments(session_factory) == []


async def test_422_message_lists_every_missing_field(client: httpx.AsyncClient) -> None:
    response = await client.post(PAYMENTS_URL, json={}, headers=headers())

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": "body.amount: Field required; body.currency: Field required; "
            "body.webhook_url: Field required",
        }
    }


async def test_422_message_does_not_echo_the_rejected_input(client: httpx.AsyncClient) -> None:
    response = await client.post(
        PAYMENTS_URL,
        json={**VALID_BODY, "currency": "SECRET-CURRENCY"},
        headers=headers(),
    )

    assert response.status_code == 422
    assert "SECRET-CURRENCY" not in response.text


@pytest.mark.parametrize(
    "content",
    [b"{not json", b"", b'{"amount": ', b"null", b"[]", b'"text"', b"42"],
)
async def test_rejects_create_with_422_when_body_is_not_a_json_object(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    content: bytes,
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        content=content,
        headers={**headers(), "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await fetch_payments(session_factory) == []


@pytest.mark.parametrize("content_type", ["text/plain", "application/x-www-form-urlencoded"])
async def test_rejects_create_with_422_when_content_type_is_not_json(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    content_type: str,
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        content=json.dumps(VALID_BODY).encode(),
        headers={**headers(), "Content-Type": content_type},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await fetch_payments(session_factory) == []


async def test_rejects_create_with_422_when_there_is_no_body_at_all(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(PAYMENTS_URL, headers=headers())

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_401_beats_400_and_422_when_api_key_is_wrong_and_everything_else_is_invalid(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        content=b"{not json",
        headers={**headers(api_key="wrong", idempotency_key=None), "Content-Type": "text/plain"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"
    assert await fetch_payments(session_factory) == []


async def test_401_beats_422_when_api_key_is_missing_and_body_is_invalid(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(PAYMENTS_URL, json={}, headers=headers(api_key=None))

    assert response.status_code == 401


async def test_400_beats_422_when_idempotency_key_is_missing_and_body_is_invalid(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(PAYMENTS_URL, json={}, headers=headers(idempotency_key=None))

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_key_required"


async def test_rejects_create_with_400_when_idempotency_key_is_empty(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key=""))

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_key_required"
    assert await fetch_payments(session_factory) == []


@pytest.mark.parametrize("length", [1, 254, 255])
async def test_accepts_idempotency_key_of_any_length_up_to_255_and_stores_it_verbatim(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], length: int
) -> None:
    key = "k" * length

    response = await client.post(
        PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key=key)
    )

    assert response.status_code == 202
    [payment] = await fetch_payments(session_factory)
    assert payment.idempotency_key == key


async def test_idempotency_keys_differing_only_in_case_are_different_keys(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key="Key"))

    second = await client.post(
        PAYMENTS_URL, json=VALID_BODY, headers=headers(idempotency_key="key")
    )

    assert second.status_code == 202
    assert len(await fetch_payments(session_factory)) == 2


async def test_treats_metadata_key_order_as_same_payload(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first_body = {**VALID_BODY, "metadata": {"a": 1, "b": {"x": 1, "y": 2}}}
    second_body = {**VALID_BODY, "metadata": {"b": {"y": 2, "x": 1}, "a": 1}}
    first = await client.post(PAYMENTS_URL, json=first_body, headers=headers())

    second = await client.post(PAYMENTS_URL, json=second_body, headers=headers())

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count_rows(session_factory, Payment) == 1
    assert await count_rows(session_factory, OutboxEvent) == 1


@pytest.mark.parametrize("repeated_amount", ["100.5", "100.50", 100.5, "1.005e2"])
async def test_treats_amount_100_5_and_100_50_as_same_payload(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    repeated_amount: Any,
) -> None:
    first = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": "100.50"}, headers=headers()
    )

    second = await client.post(
        PAYMENTS_URL, json={**VALID_BODY, "amount": repeated_amount}, headers=headers()
    )

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count_rows(session_factory, Payment) == 1


async def test_conflicting_replay_leaves_the_original_payment_untouched(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())
    [before] = await fetch_payments(session_factory)

    await client.post(
        PAYMENTS_URL,
        json={**VALID_BODY, "amount": "999.00", "description": "tampered"},
        headers=headers(),
    )

    [after] = await fetch_payments(session_factory)
    assert (after.id, str(after.amount), after.description, after.request_fingerprint) == (
        before.id,
        "100.50",
        "Order 42",
        before.request_fingerprint,
    )


async def test_replay_returns_original_status_even_after_payment_was_processed(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())
    async with session_factory() as session:
        payment = await session.get_one(Payment, uuid.UUID(first.json()["payment_id"]))
        payment.status = "succeeded"
        await session.commit()

    second = await client.post(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    assert second.status_code == 202
    assert second.json() == {**first.json(), "status": "succeeded"}
    assert await count_rows(session_factory, OutboxEvent) == 1


async def test_create_only_accepts_post(client: httpx.AsyncClient) -> None:
    response = await client.put(PAYMENTS_URL, json=VALID_BODY, headers=headers())

    assert response.status_code == 405
