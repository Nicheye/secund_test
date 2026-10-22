import asyncio
import json
import random
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import httpx
import pytest
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import app.messaging.consumer as consumer_module
from app.config import Settings, get_settings
from app.domain import Currency, PaymentSnapshot, PaymentStatus
from app.gateway import EmulatedGateway
from app.messaging.consumer import create_app
from app.messaging.topology import (
    ROUTING_KEY,
    declare_topology,
    payments_dlq,
    payments_exchange,
    payments_new_queue,
)
from app.models import Payment
from app.payment_finalization import finalize_payment
from app.webhook import HttpWebhookSender, WebhookDeliveryError

WEBHOOK_URL = "https://client.example/hook"
DEAD_LETTER_TIMEOUT_SECONDS = 30.0


class ScriptedRandom(random.Random):
    """Returns the scripted values from random(); uniform() consumes one of them as well."""

    def __init__(self, values: list[float]) -> None:
        super().__init__()
        self._values = list(values)

    def random(self) -> float:
        return self._values.pop(0)


class SleepRecorder:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


class WebhookReceiver:
    """Third-party boundary: the client's webhook endpoint."""

    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.bodies: list[dict[str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(json.loads(request.content))
        return httpx.Response(self.status_code)


def make_snapshot() -> PaymentSnapshot:
    return PaymentSnapshot(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        amount=Decimal("10.50"),
        currency=Currency.USD,
        status=PaymentStatus.PENDING,
        webhook_url=WEBHOOK_URL,
        processed_at=None,
    )


def make_gateway(
    settings: Settings, random_values: list[float], sleeper: SleepRecorder | None = None
) -> EmulatedGateway:
    return EmulatedGateway(
        settings, sleeper=sleeper or SleepRecorder(), rng=ScriptedRandom(random_values)
    )


def make_webhook_sender(settings: Settings, receiver: WebhookReceiver) -> HttpWebhookSender:
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver))
    return HttpWebhookSender(
        client, settings, sleeper=SleepRecorder(), rng=ScriptedRandom([0.0] * 10)
    )


async def insert_payment(
    session_factory: async_sessionmaker[AsyncSession],
    status: PaymentStatus = PaymentStatus.PENDING,
    processed_at: datetime | None = None,
) -> uuid.UUID:
    async with session_factory() as session:
        payment = Payment(
            amount=Decimal("10.50"),
            currency="USD",
            description="order 1",
            status=status.value,
            idempotency_key=f"key-{uuid.uuid4()}",
            request_fingerprint="fingerprint",
            webhook_url=WEBHOOK_URL,
            processed_at=processed_at,
        )
        session.add(payment)
        await session.commit()
        return payment.id


async def load_payment(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async with session_factory() as session:
        return (await session.execute(select(Payment).where(Payment.id == payment_id))).scalar_one()


# Random values per outcome: the first is consumed by uniform() for the sleep time.
SUCCESS_RANDOM = [0.0, 0.0]
FAILURE_RANDOM = [0.0, 0.99]


@pytest.mark.parametrize(
    ("outcome_roll", "expected_status"),
    [
        (0.0, PaymentStatus.SUCCEEDED),
        (0.8999, PaymentStatus.SUCCEEDED),
        (0.9, PaymentStatus.FAILED),
        (0.99, PaymentStatus.FAILED),
    ],
)
async def test_gateway_succeeds_with_configured_probability(
    settings: Settings, outcome_roll: float, expected_status: PaymentStatus
) -> None:
    gateway = make_gateway(settings, [0.0, outcome_roll])

    result = await gateway.charge(make_snapshot())

    assert result == expected_status


async def test_gateway_sleeps_a_uniform_time_within_configured_bounds(settings: Settings) -> None:
    sleeper = SleepRecorder()
    gateway = make_gateway(settings, [0.5, 0.0], sleeper)

    await gateway.charge(make_snapshot())

    assert sleeper.delays == [3.5]


async def test_successful_gateway_outcome_sets_succeeded_and_processed_at(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()

    await finalize_payment(
        session_factory,
        payment_id,
        make_gateway(settings, SUCCESS_RANDOM),
        make_webhook_sender(settings, receiver),
    )

    stored = await load_payment(session_factory, payment_id)
    assert stored.status == "succeeded"
    assert stored.processed_at is not None
    assert receiver.bodies == [
        {
            "payment_id": str(payment_id),
            "status": "succeeded",
            "amount": "10.50",
            "currency": "USD",
            "processed_at": stored.processed_at.astimezone(UTC).isoformat(),
        }
    ]


async def test_failed_gateway_outcome_sets_failed_and_notifies(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()

    await finalize_payment(
        session_factory,
        payment_id,
        make_gateway(settings, FAILURE_RANDOM),
        make_webhook_sender(settings, receiver),
    )

    stored = await load_payment(session_factory, payment_id)
    assert stored.status == "failed"
    assert stored.processed_at is not None
    assert [body["status"] for body in receiver.bodies] == ["failed"]


async def test_redelivered_message_does_not_run_gateway_again_or_change_status(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()
    sleeper = SleepRecorder()
    # Only one gateway run is scripted: a second run would exhaust the script and fail.
    gateway = make_gateway(settings, SUCCESS_RANDOM, sleeper)
    sender = make_webhook_sender(settings, receiver)
    await finalize_payment(session_factory, payment_id, gateway, sender)
    first = await load_payment(session_factory, payment_id)

    await finalize_payment(session_factory, payment_id, gateway, sender)

    second = await load_payment(session_factory, payment_id)
    assert len(sleeper.delays) == 1
    assert (second.status, second.processed_at) == (first.status, first.processed_at)
    assert [body["status"] for body in receiver.bodies] == ["succeeded", "succeeded"]


async def test_redelivery_after_webhook_failure_resends_webhook_without_gateway(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver(status_code=500)
    sleeper = SleepRecorder()
    gateway = make_gateway(settings, SUCCESS_RANDOM, sleeper)
    with pytest.raises(WebhookDeliveryError):
        await finalize_payment(
            session_factory, payment_id, gateway, make_webhook_sender(settings, receiver)
        )
    receiver.status_code = 200

    await finalize_payment(
        session_factory, payment_id, gateway, make_webhook_sender(settings, receiver)
    )

    assert len(sleeper.delays) == 1
    assert len(receiver.bodies) == 4
    assert (await load_payment(session_factory, payment_id)).status == "succeeded"


async def test_permanent_webhook_failure_raises_and_keeps_gateway_status(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver(status_code=503)

    with pytest.raises(WebhookDeliveryError):
        await finalize_payment(
            session_factory,
            payment_id,
            make_gateway(settings, FAILURE_RANDOM),
            make_webhook_sender(settings, receiver),
        )

    stored = await load_payment(session_factory, payment_id)
    assert stored.status == "failed"
    assert stored.processed_at is not None
    assert len(receiver.bodies) == 3


async def test_missing_payment_is_acknowledged_without_error(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    receiver = WebhookReceiver()
    sleeper = SleepRecorder()

    await finalize_payment(
        session_factory,
        uuid.uuid4(),
        make_gateway(settings, SUCCESS_RANDOM, sleeper),
        make_webhook_sender(settings, receiver),
    )

    assert sleeper.delays == []
    assert receiver.bodies == []


async def test_concurrent_duplicate_deliveries_store_one_outcome(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()
    # Both workers read the payment as pending before either gets its gateway answer.
    both_in_gateway = asyncio.Barrier(2)

    async def wait_for_other_worker(seconds: float) -> None:
        await both_in_gateway.wait()

    succeeding = make_gateway(settings, SUCCESS_RANDOM, wait_for_other_worker)  # type: ignore[arg-type]
    failing = make_gateway(settings, FAILURE_RANDOM, wait_for_other_worker)  # type: ignore[arg-type]
    sender = make_webhook_sender(settings, receiver)

    await asyncio.gather(
        finalize_payment(session_factory, payment_id, succeeding, sender),
        finalize_payment(session_factory, payment_id, failing, sender),
    )

    stored = await load_payment(session_factory, payment_id)
    # Which worker wins the conditional UPDATE is a race, so the winner is intentionally not
    # asserted; what matters is that exactly one final status is stored and both webhooks agree.
    assert stored.status in {"succeeded", "failed"}
    assert stored.processed_at is not None
    assert [body["status"] for body in receiver.bodies] == [stored.status, stored.status]
    assert {body["processed_at"] for body in receiver.bodies} == {
        stored.processed_at.astimezone(UTC).isoformat()
    }


async def test_already_final_payment_keeps_its_status_and_skips_gateway(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    processed_at = datetime(2026, 1, 1, tzinfo=UTC)
    payment_id = await insert_payment(session_factory, PaymentStatus.FAILED, processed_at)
    sleeper = SleepRecorder()

    await finalize_payment(
        session_factory,
        payment_id,
        make_gateway(settings, SUCCESS_RANDOM, sleeper),
        make_webhook_sender(settings, WebhookReceiver()),
    )

    stored = await load_payment(session_factory, payment_id)
    assert (stored.status, stored.processed_at) == ("failed", processed_at)
    assert sleeper.delays == []


@pytest.fixture
async def publisher(rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    broker = RabbitBroker(rabbitmq_url)
    await broker.connect()
    await declare_topology(broker)
    for queue in (payments_new_queue, payments_dlq):
        await (await broker.declare_queue(queue)).purge()
    yield broker
    await broker.stop()


async def wait_for_dead_letter(broker: RabbitBroker) -> bytes:
    queue = await broker.declare_queue(payments_dlq)
    deadline = time.monotonic() + DEAD_LETTER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        message = await queue.get(no_ack=True, fail=False)
        if message is not None:
            return bytes(message.body)
        await asyncio.sleep(0.2)
    raise AssertionError("message was not dead-lettered in time")


async def test_message_is_dead_lettered_after_three_failed_deliveries(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    publisher: RabbitBroker,
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver(status_code=500)
    consumer_settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    consumer = create_app(
        consumer_settings,
        make_gateway(settings, SUCCESS_RANDOM),
        make_webhook_sender(settings, receiver),
    )
    body = {"payment_id": str(payment_id)}

    async with consumer.lifespan_context():
        await consumer.start()
        await publisher.publish(
            body, exchange=payments_exchange, routing_key=ROUTING_KEY, persist=True
        )
        dead_lettered = await wait_for_dead_letter(publisher)

    assert json.loads(dead_lettered) == body
    # 3 deliveries, each making 3 webhook attempts.
    assert len(receiver.bodies) == 9
    assert (await load_payment(session_factory, payment_id)).status == "succeeded"


class RaisingGateway:
    """Third-party boundary: the payment provider is down."""

    def __init__(self) -> None:
        self.calls = 0

    async def charge(self, payment: PaymentSnapshot) -> PaymentStatus:
        self.calls += 1
        raise ConnectionError("provider unavailable")


@pytest.mark.parametrize("final_status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_redelivery_of_a_finished_payment_resends_the_stored_result_to_the_webhook(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    final_status: PaymentStatus,
) -> None:
    processed_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    payment_id = await insert_payment(session_factory, final_status, processed_at)
    receiver = WebhookReceiver()
    sleeper = SleepRecorder()

    await finalize_payment(
        session_factory,
        payment_id,
        make_gateway(settings, SUCCESS_RANDOM, sleeper),
        make_webhook_sender(settings, receiver),
    )

    assert sleeper.delays == []
    assert receiver.bodies == [
        {
            "payment_id": str(payment_id),
            "status": final_status.value,
            "amount": "10.50",
            "currency": "USD",
            "processed_at": "2026-01-01T12:00:00+00:00",
        }
    ]
    stored = await load_payment(session_factory, payment_id)
    assert (stored.status, stored.processed_at) == (final_status.value, processed_at)


async def test_gateway_error_leaves_payment_pending_and_sends_no_webhook(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()
    gateway = RaisingGateway()

    with pytest.raises(ConnectionError, match="provider unavailable"):
        await finalize_payment(
            session_factory, payment_id, gateway, make_webhook_sender(settings, receiver)
        )

    stored = await load_payment(session_factory, payment_id)
    assert (stored.status, stored.processed_at) == ("pending", None)
    assert receiver.bodies == []


async def test_worker_that_loses_the_compare_and_set_adopts_the_stored_outcome(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    other_worker_finished_at = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)
    receiver = WebhookReceiver()

    async def other_worker_finishes_first(seconds: float) -> None:
        async with session_factory() as session:
            payment = await session.get_one(Payment, payment_id)
            payment.status = "failed"
            payment.processed_at = other_worker_finished_at
            await session.commit()

    # This worker's gateway answers "succeeded", but the row is already "failed".
    gateway = make_gateway(settings, SUCCESS_RANDOM, other_worker_finishes_first)  # type: ignore[arg-type]

    await finalize_payment(
        session_factory, payment_id, gateway, make_webhook_sender(settings, receiver)
    )

    stored = await load_payment(session_factory, payment_id)
    assert (stored.status, stored.processed_at) == ("failed", other_worker_finished_at)
    assert receiver.bodies == [
        {
            "payment_id": str(payment_id),
            "status": "failed",
            "amount": "10.50",
            "currency": "USD",
            "processed_at": "2026-03-04T05:06:07+00:00",
        }
    ]


async def test_webhook_gets_the_processed_at_that_was_stored(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()

    await finalize_payment(
        session_factory,
        payment_id,
        make_gateway(settings, SUCCESS_RANDOM),
        make_webhook_sender(settings, receiver),
    )

    stored = await load_payment(session_factory, payment_id)
    assert stored.processed_at is not None
    assert receiver.bodies[0]["processed_at"] == stored.processed_at.astimezone(UTC).isoformat()


async def test_finalize_payment_changes_only_the_target_payment(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    target_id = await insert_payment(session_factory)
    bystander_id = await insert_payment(session_factory)

    await finalize_payment(
        session_factory,
        target_id,
        make_gateway(settings, SUCCESS_RANDOM),
        make_webhook_sender(settings, WebhookReceiver()),
    )

    assert (await load_payment(session_factory, target_id)).status == "succeeded"
    bystander = await load_payment(session_factory, bystander_id)
    assert (bystander.status, bystander.processed_at) == ("pending", None)


# --- Consumer wiring: valid messages, poison messages, module entry points -------------------


async def wait_for_status(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID, status: str
) -> None:
    deadline = time.monotonic() + DEAD_LETTER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if (await load_payment(session_factory, payment_id)).status == status:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"payment did not reach {status}")


async def wait_for_empty_queue(broker: RabbitBroker) -> None:
    """A message that is acknowledged leaves the queue (unacknowledged ones still count)."""
    deadline = time.monotonic() + DEAD_LETTER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        queue = await broker.declare_queue(payments_new_queue)
        if queue.declaration_result.message_count == 0:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("message stayed in the queue")


async def test_valid_message_is_processed_and_acknowledged(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    publisher: RabbitBroker,
) -> None:
    payment_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()
    consumer = create_app(
        settings.model_copy(update={"rabbitmq_url": rabbitmq_url}),
        make_gateway(settings, SUCCESS_RANDOM),
        make_webhook_sender(settings, receiver),
    )

    async with consumer.lifespan_context():
        await consumer.start()
        await publisher.publish(
            {"payment_id": str(payment_id)},
            exchange=payments_exchange,
            routing_key=ROUTING_KEY,
            persist=True,
        )
        await wait_for_status(session_factory, payment_id, "succeeded")
        await wait_for_empty_queue(publisher)
        dead_letter_queue = await publisher.declare_queue(payments_dlq)
        assert await dead_letter_queue.get(fail=False, no_ack=True) is None

    assert [body["status"] for body in receiver.bodies] == ["succeeded"]


@pytest.mark.parametrize(
    "poison_body",
    [
        b"not json at all",
        b"",
        b"{}",
        b'{"payment_id": null}',
        b'{"payment_id": "not-a-uuid"}',
        b'{"payment_id": 123}',
        b'["11111111-1111-4111-8111-111111111111"]',
        b'"11111111-1111-4111-8111-111111111111"',
    ],
)
async def test_poison_message_is_rejected_three_times_and_ends_in_the_dead_letter_queue(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    publisher: RabbitBroker,
    poison_body: bytes,
) -> None:
    untouched_id = await insert_payment(session_factory)
    receiver = WebhookReceiver()
    gateway_sleeper = SleepRecorder()
    consumer = create_app(
        settings.model_copy(update={"rabbitmq_url": rabbitmq_url}),
        make_gateway(settings, SUCCESS_RANDOM, gateway_sleeper),
        make_webhook_sender(settings, receiver),
    )

    async with consumer.lifespan_context():
        await consumer.start()
        await publisher.publish(
            poison_body,
            exchange=payments_exchange,
            routing_key=ROUTING_KEY,
            persist=True,
            content_type="application/json",
        )
        dead_lettered = await wait_for_dead_letter(publisher)

    assert dead_lettered == poison_body
    assert gateway_sleeper.delays == []
    assert receiver.bodies == []
    assert (await load_payment(session_factory, untouched_id)).status == "pending"
    main_queue = await publisher.declare_queue(payments_new_queue)
    assert await main_queue.get(fail=False, no_ack=True) is None


async def test_gateway_error_message_is_dead_lettered_after_three_deliveries(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    publisher: RabbitBroker,
) -> None:
    payment_id = await insert_payment(session_factory)
    gateway = RaisingGateway()
    consumer = create_app(
        settings.model_copy(update={"rabbitmq_url": rabbitmq_url}),
        gateway,
        make_webhook_sender(settings, WebhookReceiver()),
    )
    body = {"payment_id": str(payment_id)}

    async with consumer.lifespan_context():
        await consumer.start()
        await publisher.publish(
            body, exchange=payments_exchange, routing_key=ROUTING_KEY, persist=True
        )
        dead_lettered = await wait_for_dead_letter(publisher)

    assert json.loads(dead_lettered) == body
    assert gateway.calls == 3
    assert (await load_payment(session_factory, payment_id)).status == "pending"


def test_module_attribute_app_builds_the_consumer_from_environment(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", settings.database_url)
    monkeypatch.setenv("RABBITMQ_URL", settings.rabbitmq_url)
    monkeypatch.setenv("API_KEY", settings.api_key)
    get_settings.cache_clear()

    consumer_app = consumer_module.app

    assert isinstance(consumer_app, FastStream)
    get_settings.cache_clear()


def test_unknown_module_attribute_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="no_such_attribute"):
        consumer_module.no_such_attribute  # noqa: B018


async def test_stopping_the_consumer_closes_the_http_client_and_disposes_the_engine(
    settings: Settings, rabbitmq_url: str
) -> None:
    consumer = create_app(
        settings.model_copy(update={"rabbitmq_url": rabbitmq_url}),
        make_gateway(settings, SUCCESS_RANDOM),
        make_webhook_sender(settings, WebhookReceiver()),
    )

    with (
        patch.object(httpx.AsyncClient, "aclose", autospec=True) as client_close,
        patch.object(AsyncEngine, "dispose", autospec=True) as engine_dispose,
    ):
        async with consumer.lifespan_context():
            await consumer.start()
            await consumer.stop()

    client_close.assert_awaited_once()
    engine_dispose.assert_awaited_once()


class BlockingGateway:
    """Third-party boundary: every charge waits until released, so concurrency is observable."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.in_flight = 0
        self.max_in_flight = 0

    async def charge(self, payment: PaymentSnapshot) -> PaymentStatus:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await self.release.wait()
        finally:
            self.in_flight -= 1
        return PaymentStatus.SUCCEEDED


async def wait_for_in_flight(gateway: BlockingGateway, count: int) -> None:
    deadline = time.monotonic() + DEAD_LETTER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if gateway.in_flight >= count:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"fewer than {count} payments reached the gateway")


async def test_consumer_handles_at_most_prefetch_count_messages_at_the_same_time(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    publisher: RabbitBroker,
) -> None:
    payment_ids = [await insert_payment(session_factory) for _ in range(5)]
    gateway = BlockingGateway()
    consumer = create_app(
        settings.model_copy(update={"rabbitmq_url": rabbitmq_url, "consumer_prefetch_count": 2}),
        gateway,
        make_webhook_sender(settings, WebhookReceiver()),
    )

    async with consumer.lifespan_context():
        await consumer.start()
        for payment_id in payment_ids:
            await publisher.publish(
                {"payment_id": str(payment_id)},
                exchange=payments_exchange,
                routing_key=ROUTING_KEY,
                persist=True,
            )
        await wait_for_in_flight(gateway, 2)
        # Bounded negative-proof wait: a third message must NOT be delivered, and absence can
        # only be shown by giving it time to arrive if the limit were not enforced.
        await asyncio.sleep(0.5)
        in_flight_while_blocked = gateway.in_flight
        gateway.release.set()
        for payment_id in payment_ids:
            await wait_for_status(session_factory, payment_id, "succeeded")

    assert in_flight_while_blocked == 2
    assert gateway.max_in_flight == 2
