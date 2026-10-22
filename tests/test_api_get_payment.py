import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment

PAYMENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
PROCESSED_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


async def insert_payment(
    session_factory: async_sessionmaker[AsyncSession], **overrides: object
) -> Payment:
    values: dict[str, object] = {
        "id": PAYMENT_ID,
        "amount": Decimal("100.50"),
        "currency": "USD",
        "description": "Order 42",
        "payment_metadata": {"order_id": "42"},
        "status": "succeeded",
        "idempotency_key": "key-1",
        "request_fingerprint": "fingerprint",
        "webhook_url": "https://merchant.example.com/hooks/payments",
        "processed_at": PROCESSED_AT,
    }
    payment = Payment(**{**values, **overrides})
    async with session_factory() as session:
        session.add(payment)
        await session.commit()
    return payment


async def test_returns_payment_details_for_existing_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    stored = await insert_payment(session_factory)

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers={"X-API-Key": "test-key"})

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": "11111111-1111-4111-8111-111111111111",
        "amount": "100.50",
        "currency": "USD",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "status": "succeeded",
        "webhook_url": "https://merchant.example.com/hooks/payments",
        "created_at": stored.created_at.isoformat().replace("+00:00", "Z"),
        "processed_at": "2026-01-02T03:04:05Z",
    }


async def test_returns_null_processed_at_for_pending_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await insert_payment(session_factory, status="pending", processed_at=None)

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers={"X-API-Key": "test-key"})

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["processed_at"] is None


async def test_returns_404_for_unknown_payment_id(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await insert_payment(session_factory)
    unknown_id = "22222222-2222-4222-8222-222222222222"

    response = await client.get(f"/api/v1/payments/{unknown_id}", headers={"X-API-Key": "test-key"})

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "payment_not_found", "message": "Payment not found"}
    }


@pytest.mark.parametrize("malformed_id", ["not-a-uuid", "123", "1111-1111", "zzzzzzzz"])
async def test_returns_404_for_malformed_payment_id(
    client: httpx.AsyncClient, malformed_id: str
) -> None:
    response = await client.get(
        f"/api/v1/payments/{malformed_id}", headers={"X-API-Key": "test-key"}
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "payment_not_found"


@pytest.mark.parametrize("api_key", [None, "", "wrong-key"])
async def test_rejects_get_with_401_when_api_key_is_missing_or_wrong(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    api_key: str | None,
) -> None:
    await insert_payment(session_factory)
    request_headers = {} if api_key is None else {"X-API-Key": api_key}

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=request_headers)

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


def api_headers() -> dict[str, str]:
    return {"X-API-Key": "test-key"}


@pytest.mark.parametrize(
    ("status", "processed_at", "expected_processed_at"),
    [
        ("pending", None, None),
        ("succeeded", PROCESSED_AT, "2026-01-02T03:04:05Z"),
        ("failed", PROCESSED_AT, "2026-01-02T03:04:05Z"),
    ],
)
async def test_returns_every_field_for_payment_in_each_status(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    status: str,
    processed_at: datetime | None,
    expected_processed_at: str | None,
) -> None:
    stored = await insert_payment(session_factory, status=status, processed_at=processed_at)

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=api_headers())

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": "11111111-1111-4111-8111-111111111111",
        "amount": "100.50",
        "currency": "USD",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "status": status,
        "webhook_url": "https://merchant.example.com/hooks/payments",
        "created_at": stored.created_at.isoformat().replace("+00:00", "Z"),
        "processed_at": expected_processed_at,
    }


@pytest.mark.parametrize(
    ("amount", "currency", "expected_amount"),
    [
        (Decimal("0.01"), "RUB", "0.01"),
        (Decimal("5"), "EUR", "5.00"),
        (Decimal("1000000000.00"), "USD", "1000000000.00"),
        (Decimal("123456789.10"), "RUB", "123456789.10"),
    ],
)
async def test_returns_amount_as_a_two_decimal_string_with_its_currency(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    amount: Decimal,
    currency: str,
    expected_amount: str,
) -> None:
    await insert_payment(session_factory, amount=amount, currency=currency)

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=api_headers())

    assert response.json()["amount"] == expected_amount
    assert response.json()["currency"] == currency


@pytest.mark.parametrize(
    "metadata", [{}, {"nested": {"list": [1, {"a": None}], "flag": False}, "n": 1.5}]
)
async def test_returns_metadata_exactly_as_stored(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    metadata: dict[str, object],
) -> None:
    await insert_payment(session_factory, payment_metadata=metadata)

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=api_headers())

    assert response.json()["metadata"] == metadata


async def test_returns_only_the_requested_payment_when_others_exist(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await insert_payment(session_factory, description="requested")
    await insert_payment(
        session_factory,
        id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
        idempotency_key="key-2",
        description="distractor",
    )

    response = await client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=api_headers())

    assert response.json()["description"] == "requested"


async def test_finds_payment_when_id_is_written_in_uppercase(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await insert_payment(session_factory)

    response = await client.get(
        f"/api/v1/payments/{str(PAYMENT_ID).upper()}", headers=api_headers()
    )

    assert response.status_code == 200
    assert response.json()["payment_id"] == str(PAYMENT_ID)


@pytest.mark.parametrize("path_id", ["not-a-uuid", "22222222-2222-4222-8222-222222222222"])
async def test_401_beats_404_when_api_key_is_wrong(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    path_id: str,
) -> None:
    response = await client.get(f"/api/v1/payments/{path_id}", headers={"X-API-Key": "wrong"})

    assert response.status_code == 401
    assert response.json() == {
        "error": {"code": "unauthorized", "message": "Missing or invalid API key"}
    }


async def test_returns_404_not_found_body_for_unknown_route(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/unknown", headers=api_headers())

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "not_found", "message": "Not Found"}}


async def test_unknown_route_returns_404_even_without_api_key(client: httpx.AsyncClient) -> None:
    response = await client.get("/nothing-here")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
async def test_returns_405_http_error_body_for_unsupported_method_on_payment(
    client: httpx.AsyncClient, method: str
) -> None:
    response = await client.request(method, f"/api/v1/payments/{PAYMENT_ID}", headers=api_headers())

    assert response.status_code == 405
    assert response.json() == {"error": {"code": "http_error", "message": "Method Not Allowed"}}


async def test_returns_405_for_listing_payments_because_no_list_endpoint_exists(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/payments", headers=api_headers())

    assert response.status_code == 405
    assert response.json()["error"]["code"] == "http_error"
