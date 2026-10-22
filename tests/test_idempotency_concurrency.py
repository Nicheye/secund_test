import asyncio
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, Payment

PARALLEL_REQUESTS = 20
BODY: dict[str, Any] = {
    "amount": "250.00",
    "currency": "EUR",
    "description": "Concurrent order",
    "metadata": {},
    "webhook_url": "https://merchant.example.com/hook",
}
HEADERS = {"X-API-Key": "test-key", "Idempotency-Key": "parallel-key"}


async def count_rows(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model)) or 0


async def test_parallel_duplicates_with_same_payload_create_exactly_one_payment_and_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    requests = [
        client.post("/api/v1/payments", json=BODY, headers=HEADERS)
        for _ in range(PARALLEL_REQUESTS)
    ]

    responses = await asyncio.gather(*requests)

    assert [response.status_code for response in responses] == [202] * PARALLEL_REQUESTS
    assert len({response.text for response in responses}) == 1
    assert await count_rows(session_factory, Payment) == 1
    assert await count_rows(session_factory, OutboxEvent) == 1


async def test_parallel_requests_with_same_key_and_different_payloads_yield_one_winner(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    requests = [
        client.post(
            "/api/v1/payments",
            json={**BODY, "amount": f"{index + 1}.00"},
            headers=HEADERS,
        )
        for index in range(PARALLEL_REQUESTS)
    ]

    responses = await asyncio.gather(*requests)

    status_codes = sorted(response.status_code for response in responses)
    assert status_codes == [202] + [409] * (PARALLEL_REQUESTS - 1)
    assert await count_rows(session_factory, Payment) == 1
    assert await count_rows(session_factory, OutboxEvent) == 1
    winner = next(response for response in responses if response.status_code == 202)
    async with session_factory() as session:
        stored = await session.scalar(select(Payment))
    assert stored is not None
    assert str(stored.id) == winner.json()["payment_id"]
