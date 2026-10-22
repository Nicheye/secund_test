import asyncio
import json
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.domain import PaymentStatus
from app.messaging.consumer import create_app as create_consumer_app
from app.messaging.relay import Relay
from app.messaging.topology import declare_topology, payments_dlq, payments_new_queue
from app.models import OutboxEvent, Payment
from app.webhook import HttpWebhookSender

CONSUME_TIMEOUT_SECONDS = 30.0


class FakeGateway:
    """Third-party boundary: answers at once, without the emulated delay."""

    async def process(self, payment: Payment) -> PaymentStatus:
        return PaymentStatus.SUCCEEDED


class RecordingWebhookSender:
    """Third-party boundary: records what would have been POSTed to the client."""

    def __init__(self) -> None:
        self.payments: list[Payment] = []

    async def send(self, payment: Payment) -> None:
        self.payments.append(payment)


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    rabbit_broker = RabbitBroker(rabbitmq_url)
    await rabbit_broker.connect()
    await declare_topology(rabbit_broker)
    for queue in (payments_new_queue, payments_dlq):
        await (await rabbit_broker.declare_queue(queue)).purge()
    yield rabbit_broker
    await rabbit_broker.stop()


async def wait_until_processed(
    session_factory: async_sessionmaker[AsyncSession], sender: RecordingWebhookSender
) -> None:
    deadline = time.monotonic() + CONSUME_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if sender.payments:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("payment was not consumed in time")


async def test_created_payment_is_relayed_consumed_and_notified_exactly_once(
    client: httpx.AsyncClient,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    broker: RabbitBroker,
) -> None:
    sender = RecordingWebhookSender()
    consumer = create_consumer_app(
        settings.model_copy(update={"rabbitmq_url": rabbitmq_url}), FakeGateway(), sender
    )
    relay = Relay(session_factory, broker, poll_interval_seconds=0.05, batch_size=50)

    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "100.50",
            "currency": "USD",
            "description": "Order 42",
            "webhook_url": "https://merchant.example.com/hooks/payments",
        },
        headers={"X-API-Key": "test-key", "Idempotency-Key": "e2e-key"},
    )
    payment_id = response.json()["payment_id"]
    published = await relay.publish_batch()
    async with consumer.lifespan_context():
        await consumer.start()
        await wait_until_processed(session_factory, sender)

    async with session_factory() as session:
        stored = (await session.execute(select(Payment))).scalar_one()
        event = (await session.execute(select(OutboxEvent))).scalar_one()
    assert response.status_code == 202
    assert published == 1
    assert event.published_at is not None
    assert str(stored.id) == payment_id
    assert stored.status == "succeeded"
    assert stored.processed_at is not None
    assert stored.processed_at <= datetime.now(UTC)
    assert [(str(p.id), p.status, p.webhook_url) for p in sender.payments] == [
        (payment_id, "succeeded", "https://merchant.example.com/hooks/payments")
    ]
    assert event.payload == {"payment_id": payment_id}


class DecliningGateway:
    """Third-party boundary: the provider declines every payment."""

    async def process(self, payment: Payment) -> PaymentStatus:
        return PaymentStatus.FAILED


async def test_declined_payment_is_stored_as_failed_and_client_is_notified_with_failed_status(
    client: httpx.AsyncClient,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    broker: RabbitBroker,
) -> None:
    delivered: list[dict[str, str]] = []

    def client_webhook_endpoint(request: httpx.Request) -> httpx.Response:
        delivered.append(json.loads(request.content))
        return httpx.Response(204)

    consumer_settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    webhook_sender = HttpWebhookSender(
        httpx.AsyncClient(transport=httpx.MockTransport(client_webhook_endpoint)),
        consumer_settings,
    )
    consumer = create_consumer_app(consumer_settings, DecliningGateway(), webhook_sender)
    relay = Relay(session_factory, broker, poll_interval_seconds=0.05, batch_size=50)

    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "7.25",
            "currency": "EUR",
            "description": "Declined order",
            "webhook_url": "https://merchant.example.com/hooks/payments",
        },
        headers={"X-API-Key": "test-key", "Idempotency-Key": "e2e-declined"},
    )
    payment_id = response.json()["payment_id"]
    await relay.publish_batch()
    async with consumer.lifespan_context():
        await consumer.start()
        await wait_until_delivered(delivered)

    async with session_factory() as session:
        stored = (await session.execute(select(Payment))).scalar_one()
    get_response = await client.get(
        f"/api/v1/payments/{payment_id}", headers={"X-API-Key": "test-key"}
    )
    assert stored.status == "failed"
    assert stored.processed_at is not None
    assert delivered == [
        {
            "payment_id": payment_id,
            "status": "failed",
            "amount": "7.25",
            "currency": "EUR",
            "processed_at": stored.processed_at.astimezone(UTC).isoformat(),
        }
    ]
    assert get_response.json()["status"] == "failed"
    assert get_response.json()["processed_at"] is not None


async def wait_until_delivered(delivered: list[dict[str, str]]) -> None:
    deadline = time.monotonic() + CONSUME_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if delivered:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("webhook was not delivered in time")
