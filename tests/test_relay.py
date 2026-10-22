import asyncio
import json
import logging
import os
import signal
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import aio_pika
import pytest
from aio_pika.abc import AbstractIncomingMessage
from aiormq.exceptions import AMQPError, ChannelPreconditionFailed, DeliveryError
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.messaging.relay import Relay, connect_with_retry, create_broker, run_relay
from app.messaging.topology import (
    ROUTING_KEY,
    declare_topology,
    payments_dlq,
    payments_dlx,
    payments_exchange,
    payments_new_queue,
)
from app.models import OutboxEvent, Payment


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    rabbit_broker = RabbitBroker(rabbitmq_url)
    await rabbit_broker.connect()
    await declare_topology(rabbit_broker)
    for queue in (payments_new_queue, payments_dlq):
        await (await rabbit_broker.declare_queue(queue)).purge()
    yield rabbit_broker
    await rabbit_broker.stop()


def make_relay(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker, batch_size: int = 50
) -> Relay:
    return Relay(session_factory, broker, poll_interval_seconds=0.05, batch_size=batch_size)


async def insert_outbox_events(
    session_factory: async_sessionmaker[AsyncSession], count: int
) -> list[uuid.UUID]:
    event_ids: list[uuid.UUID] = []
    async with session_factory() as session:
        for number in range(count):
            payment = Payment(
                amount=10,
                currency="USD",
                description=f"payment {number}",
                idempotency_key=f"key-{uuid.uuid4()}",
                request_fingerprint="fingerprint",
                webhook_url="https://client.example/hook",
            )
            session.add(payment)
            await session.flush()
            event = OutboxEvent(
                aggregate_id=payment.id,
                event_type="payment.created",
                payload={"payment_id": str(payment.id)},
            )
            session.add(event)
            await session.flush()
            event_ids.append(event.id)
        await session.commit()
    return event_ids


async def fetch_published_flags(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[uuid.UUID, bool]:
    async with session_factory() as session:
        rows = await session.scalars(select(OutboxEvent))
        return {row.id: row.published_at is not None for row in rows}


async def drain_queue(broker: RabbitBroker) -> list[AbstractIncomingMessage]:
    queue = await broker.declare_queue(payments_new_queue)
    messages: list[AbstractIncomingMessage] = []
    while (message := await queue.get(fail=False)) is not None:
        await message.ack()
        messages.append(message)
    return messages


async def test_publishes_row_once_with_confirmed_persistent_message_and_marks_it_published(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)
    relay = make_relay(session_factory, broker)

    first_count = await relay.publish_batch()
    second_count = await relay.publish_batch()

    messages = await drain_queue(broker)
    assert (first_count, second_count) == (1, 0)
    assert len(messages) == 1
    async with session_factory() as session:
        event = await session.get_one(OutboxEvent, event_id)
    assert messages[0].message_id == str(event_id)
    assert messages[0].routing_key == ROUTING_KEY
    assert messages[0].delivery_mode == 2
    assert json.loads(messages[0].body) == event.payload
    assert event.published_at is not None


async def test_leaves_row_unpublished_when_broker_rejects_publish_and_publishes_it_later(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)
    relay = make_relay(session_factory, broker)

    with (
        patch.object(broker, "publish", side_effect=AMQPError("broker down")),
        pytest.raises(AMQPError),
    ):
        await relay.publish_batch()

    assert await fetch_published_flags(session_factory) == {event_id: False}
    assert await drain_queue(broker) == []

    published = await relay.publish_batch()

    assert published == 1
    assert await fetch_published_flags(session_factory) == {event_id: True}
    assert [m.message_id for m in await drain_queue(broker)] == [str(event_id)]


async def test_keeps_confirmed_rows_marked_when_a_later_publish_in_the_batch_fails(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    await insert_outbox_events(session_factory, 3)
    relay = make_relay(session_factory, broker)
    real_publish = broker.publish
    calls = 0

    async def publish_failing_on_second_call(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise AMQPError("broker down")
        return await real_publish(*args, **kwargs)

    with (
        patch.object(broker, "publish", publish_failing_on_second_call),
        pytest.raises(AMQPError),
    ):
        await relay.publish_batch()

    flags = await fetch_published_flags(session_factory)
    assert sorted(flags.values()) == [False, False, True]
    assert len(await drain_queue(broker)) == 1


async def test_does_not_publish_again_rows_that_are_already_published(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    await insert_outbox_events(session_factory, 2)
    relay = make_relay(session_factory, broker)
    await relay.publish_batch()
    await drain_queue(broker)

    [new_event_id] = await insert_outbox_events(session_factory, 1)
    published = await relay.publish_batch()

    assert published == 1
    assert [m.message_id for m in await drain_queue(broker)] == [str(new_event_id)]


async def test_two_concurrent_relays_never_publish_the_same_row_twice(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    event_ids = await insert_outbox_events(session_factory, 30)
    real_publish = broker.publish
    published_message_ids: list[str] = []

    async def slow_recording_publish(*args: Any, **kwargs: Any) -> Any:
        # The pause keeps both relays inside their batch at the same time.
        await asyncio.sleep(0.02)
        published_message_ids.append(kwargs["message_id"])
        return await real_publish(*args, **kwargs)

    first_relay = make_relay(session_factory, broker, batch_size=20)
    second_relay = make_relay(session_factory, broker, batch_size=20)

    with patch.object(broker, "publish", slow_recording_publish):
        counts = await asyncio.gather(first_relay.publish_batch(), second_relay.publish_batch())

    assert sum(counts) == 30
    assert min(counts) > 0
    assert sorted(published_message_ids) == sorted(str(event_id) for event_id in event_ids)
    assert sorted(str(m.message_id) for m in await drain_queue(broker)) == sorted(
        str(event_id) for event_id in event_ids
    )


async def test_nacked_message_is_dead_lettered_after_exactly_three_delivery_attempts(
    broker: RabbitBroker,
) -> None:
    await broker.publish(
        {"payment_id": "p-1"},
        exchange="payments",
        routing_key=ROUTING_KEY,
        message_id="m-1",
        persist=True,
    )
    main_queue = await broker.declare_queue(payments_new_queue)
    dead_letter_queue = await broker.declare_queue(payments_dlq)
    attempts = 0

    for _ in range(10):
        message = await main_queue.get(fail=False)
        if message is None:
            await asyncio.sleep(0.2)
            continue
        attempts += 1
        await message.nack(requeue=True)

    dead_letter = await dead_letter_queue.get(fail=False)
    assert attempts == 3
    assert dead_letter is not None
    assert dead_letter.message_id == "m-1"
    assert await main_queue.get(fail=False) is None


def test_routing_key_and_topology_names_are_the_wire_contract() -> None:
    assert ROUTING_KEY == "payments.new"
    assert payments_exchange.name == "payments"
    assert payments_new_queue.name == "payments.new"
    assert payments_dlx.name == "payments.dlx"
    assert payments_dlq.name == "payments.new.dlq"


async def test_unroutable_message_leaves_row_unpublished_instead_of_marking_it_published(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker, rabbitmq_url: str
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)
    relay_broker = create_broker(rabbitmq_url)
    await relay_broker.connect()
    relay = make_relay(session_factory, relay_broker)
    main_queue = await broker.declare_queue(payments_new_queue)
    exchange = await broker.declare_exchange(payments_exchange)
    await main_queue.unbind(exchange, routing_key=ROUTING_KEY)

    try:
        with pytest.raises(DeliveryError):
            await relay.publish_batch()
        assert await fetch_published_flags(session_factory) == {event_id: False}
    finally:
        await main_queue.bind(exchange, routing_key=ROUTING_KEY)

    try:
        assert await relay.publish_batch() == 1
        assert await fetch_published_flags(session_factory) == {event_id: True}
    finally:
        await relay_broker.stop()


# --- publish_batch edge cases -------------------------------------------------------------


async def insert_event_at(
    session_factory: async_sessionmaker[AsyncSession],
    created_at: datetime,
    event_id: uuid.UUID | None = None,
) -> uuid.UUID:
    async with session_factory() as session:
        payment = Payment(
            amount=10,
            currency="USD",
            description="payment",
            idempotency_key=f"key-{uuid.uuid4()}",
            request_fingerprint="fingerprint",
            webhook_url="https://client.example/hook",
        )
        session.add(payment)
        await session.flush()
        event = OutboxEvent(
            id=event_id or uuid.uuid4(),
            aggregate_id=payment.id,
            event_type="payment.created",
            payload={"payment_id": str(payment.id)},
            created_at=created_at,
        )
        session.add(event)
        await session.commit()
        return event.id


async def test_publish_batch_does_nothing_when_outbox_is_empty(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    relay = make_relay(session_factory, broker)

    published = await relay.publish_batch()

    assert published == 0
    assert await drain_queue(broker) == []


async def test_publish_batch_respects_the_batch_size_and_takes_the_oldest_rows_first(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    # Inserted newest first so that insertion order differs from age order.
    newest = await insert_event_at(session_factory, base + timedelta(minutes=4))
    third = await insert_event_at(session_factory, base + timedelta(minutes=3))
    second = await insert_event_at(session_factory, base + timedelta(minutes=2))
    first = await insert_event_at(session_factory, base + timedelta(minutes=1))
    relay = make_relay(session_factory, broker, batch_size=3)

    first_batch = await relay.publish_batch()
    after_first_batch = await fetch_published_flags(session_factory)
    second_batch = await relay.publish_batch()
    third_batch = await relay.publish_batch()

    assert (first_batch, second_batch, third_batch) == (3, 1, 0)
    assert after_first_batch == {first: True, second: True, third: True, newest: False}
    messages = await drain_queue(broker)
    assert [m.message_id for m in messages] == [str(i) for i in (first, second, third, newest)]


async def test_publish_batch_orders_rows_with_equal_creation_time_by_id(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    same_time = datetime(2026, 1, 1, tzinfo=UTC)
    high = await insert_event_at(
        session_factory, same_time, uuid.UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
    )
    low = await insert_event_at(
        session_factory, same_time, uuid.UUID("00000000-0000-4000-8000-000000000001")
    )

    await make_relay(session_factory, broker).publish_batch()

    assert [m.message_id for m in await drain_queue(broker)] == [str(low), str(high)]


async def test_publish_batch_skips_published_rows_and_publishes_only_unpublished_ones(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    already_published = await insert_event_at(session_factory, base)
    pending = await insert_event_at(session_factory, base + timedelta(minutes=1))
    async with session_factory() as session:
        stored = await session.get_one(OutboxEvent, already_published)
        stored.published_at = base
        await session.commit()

    published = await make_relay(session_factory, broker).publish_batch()

    assert published == 1
    assert [m.message_id for m in await drain_queue(broker)] == [str(pending)]
    async with session_factory() as session:
        untouched = await session.get_one(OutboxEvent, already_published)
    assert untouched.published_at == base


# --- failing rows (head-of-line blocking) -----------------------------------------------------


async def fetch_outbox_event(
    session_factory: async_sessionmaker[AsyncSession], event_id: uuid.UUID
) -> OutboxEvent:
    async with session_factory() as session:
        return await session.get_one(OutboxEvent, event_id)


def publish_failing_for(
    broker: RabbitBroker, poison_id: uuid.UUID, error: Exception
) -> Callable[..., Awaitable[Any]]:
    real_publish = broker.publish

    async def publish(*args: Any, **kwargs: Any) -> Any:
        if kwargs["message_id"] == str(poison_id):
            raise error
        return await real_publish(*args, **kwargs)

    return publish


async def test_a_row_that_cannot_be_published_does_not_block_the_rows_behind_it(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    poison = await insert_event_at(session_factory, base)
    healthy_first = await insert_event_at(session_factory, base + timedelta(minutes=1))
    healthy_second = await insert_event_at(session_factory, base + timedelta(minutes=2))
    relay = make_relay(session_factory, broker)

    with patch.object(broker, "publish", publish_failing_for(broker, poison, AMQPError("nope"))):
        with pytest.raises(AMQPError):
            await relay.publish_batch()
        # The poison row now sorts last: the healthy rows go out before it fails again.
        with pytest.raises(AMQPError):
            await relay.publish_batch()

    assert await fetch_published_flags(session_factory) == {
        poison: False,
        healthy_first: True,
        healthy_second: True,
    }
    assert [m.message_id for m in await drain_queue(broker)] == [
        str(healthy_first),
        str(healthy_second),
    ]


async def test_a_non_broker_publish_error_is_recorded_and_later_rows_still_publish(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    poison = await insert_event_at(session_factory, base)
    healthy = await insert_event_at(session_factory, base + timedelta(minutes=1))
    relay = make_relay(session_factory, broker)

    failing_publish = publish_failing_for(broker, poison, TypeError("not serialisable"))
    with patch.object(broker, "publish", failing_publish):
        with pytest.raises(TypeError):
            await relay.publish_batch()
        with pytest.raises(TypeError):
            await relay.publish_batch()
    stored_poison = await fetch_outbox_event(session_factory, poison)

    assert (stored_poison.publish_attempts, stored_poison.last_error) == (
        2,
        "TypeError: not serialisable",
    )
    assert await fetch_published_flags(session_factory) == {poison: False, healthy: True}
    assert [m.message_id for m in await drain_queue(broker)] == [str(healthy)]


async def test_failed_publish_increments_attempts_and_stores_the_error_of_that_row_only(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    poison = await insert_event_at(session_factory, base)
    bystander = await insert_event_at(session_factory, base + timedelta(minutes=1))
    relay = make_relay(session_factory, broker, batch_size=1)

    with (
        patch.object(
            broker, "publish", publish_failing_for(broker, poison, AMQPError("channel closed"))
        ),
        pytest.raises(AMQPError),
    ):
        await relay.publish_batch()
    stored_poison = await fetch_outbox_event(session_factory, poison)
    stored_bystander = await fetch_outbox_event(session_factory, bystander)

    assert (stored_poison.publish_attempts, stored_poison.last_error) == (
        1,
        "AMQPError: channel closed",
    )
    assert (stored_bystander.publish_attempts, stored_bystander.last_error) == (0, None)
    assert stored_poison.published_at is None


async def test_attempts_keep_growing_while_the_row_keeps_failing(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)
    relay = make_relay(session_factory, broker)

    for _ in range(3):
        with (
            patch.object(broker, "publish", side_effect=ConnectionError("refused")),
            pytest.raises(ConnectionError),
        ):
            await relay.publish_batch()

    stored = await fetch_outbox_event(session_factory, event_id)
    assert (stored.publish_attempts, stored.last_error) == (3, "ConnectionError: refused")


async def test_the_stored_error_is_truncated(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)

    with (
        patch.object(broker, "publish", side_effect=AMQPError("x" * 5000)),
        pytest.raises(AMQPError),
    ):
        await make_relay(session_factory, broker).publish_batch()

    stored = await fetch_outbox_event(session_factory, event_id)
    assert stored.last_error == ("AMQPError: " + "x" * 5000)[:1000]


async def test_a_failing_row_is_published_later_once_the_broker_accepts_it(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    poison = await insert_event_at(session_factory, base)
    healthy = await insert_event_at(session_factory, base + timedelta(minutes=1))
    relay = make_relay(session_factory, broker)
    with patch.object(broker, "publish", publish_failing_for(broker, poison, AMQPError("nope"))):
        with pytest.raises(AMQPError):
            await relay.publish_batch()
        with pytest.raises(AMQPError):
            await relay.publish_batch()

    published = await relay.publish_batch()

    assert published == 1
    assert await fetch_published_flags(session_factory) == {poison: True, healthy: True}
    stored = await fetch_outbox_event(session_factory, poison)
    assert stored.publish_attempts == 2
    assert [m.message_id for m in await drain_queue(broker)] == [str(healthy), str(poison)]


async def test_rows_with_fewer_failed_attempts_are_published_first_whatever_their_age(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    old_but_failing = await insert_event_at(session_factory, base)
    new_and_healthy = await insert_event_at(session_factory, base + timedelta(minutes=5))
    async with session_factory() as session:
        stored = await session.get_one(OutboxEvent, old_but_failing)
        stored.publish_attempts = 2
        await session.commit()

    await make_relay(session_factory, broker, batch_size=1).publish_batch()

    assert await fetch_published_flags(session_factory) == {
        old_but_failing: False,
        new_and_healthy: True,
    }


# --- run loop --------------------------------------------------------------------------------

WAIT_TIMEOUT_SECONDS = 30.0


async def wait_until_all_published(session_factory: async_sessionmaker[AsyncSession]) -> None:
    deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        flags = await fetch_published_flags(session_factory)
        if flags and all(flags.values()):
            return
        await asyncio.sleep(0.02)
    raise AssertionError("outbox rows were not published in time")


async def wait_until_published(
    session_factory: async_sessionmaker[AsyncSession], event_id: uuid.UUID
) -> None:
    deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if (await fetch_published_flags(session_factory)).get(event_id):
            return
        await asyncio.sleep(0.02)
    raise AssertionError("outbox row was not published in time")


async def test_run_publishes_following_full_batches_without_waiting_for_the_poll_interval(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    await insert_outbox_events(session_factory, 5)
    # The poll interval is an hour: only a full batch continuing at once can publish all 5.
    relay = Relay(session_factory, broker, poll_interval_seconds=3600, batch_size=2)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    await wait_until_all_published(session_factory)
    stop.set()
    await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert len(await drain_queue(broker)) == 5


async def test_run_stops_promptly_when_stop_is_set_during_the_poll_wait(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    relay = Relay(session_factory, broker, poll_interval_seconds=3600, batch_size=10)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    stop.set()
    await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert task.done()
    assert task.exception() is None


async def test_run_publishes_nothing_when_stop_is_already_set(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    await insert_outbox_events(session_factory, 1)
    stop = asyncio.Event()
    stop.set()

    await make_relay(session_factory, broker).run(stop)

    assert list((await fetch_published_flags(session_factory)).values()) == [False]
    assert await drain_queue(broker) == []


async def test_run_picks_up_rows_that_arrive_after_it_started_polling(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    relay = make_relay(session_factory, broker)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    [event_id] = await insert_outbox_events(session_factory, 1)
    await wait_until_all_published(session_factory)
    stop.set()
    await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert [m.message_id for m in await drain_queue(broker)] == [str(event_id)]


async def test_run_survives_a_broker_error_and_publishes_on_the_next_poll(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)
    real_publish = broker.publish
    calls = 0

    async def publish_failing_once(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise AMQPError("broker down")
        return await real_publish(*args, **kwargs)

    stop = asyncio.Event()
    with patch.object(broker, "publish", publish_failing_once):
        task = asyncio.create_task(make_relay(session_factory, broker).run(stop))
        await wait_until_all_published(session_factory)
        stop.set()
        await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert calls == 2
    assert "Outbox publish failed; unpublished rows will be retried" in caplog.text
    assert [m.message_id for m in await drain_queue(broker)] == [str(event_id)]


async def test_run_keeps_polling_past_a_non_broker_error_and_publishes_the_rows_behind_it(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    poison = await insert_event_at(session_factory, base)
    healthy = await insert_event_at(session_factory, base + timedelta(minutes=1))
    stop = asyncio.Event()

    with patch.object(
        broker, "publish", publish_failing_for(broker, poison, TypeError("not serialisable"))
    ):
        task = asyncio.create_task(make_relay(session_factory, broker).run(stop))
        await wait_until_published(session_factory, healthy)
        stop.set()
        await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert "Outbox publish failed; unpublished rows will be retried" in caplog.text
    assert (await fetch_outbox_event(session_factory, poison)).publish_attempts >= 1
    assert [m.message_id for m in await drain_queue(broker)] == [str(healthy)]


# --- connect_with_retry and run_relay ---------------------------------------------------------


@pytest.fixture
def fast_connect_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.messaging.relay.CONNECT_RETRY_SECONDS", 0.01)


@pytest.mark.parametrize("error", [AMQPError("down"), ConnectionError("refused"), TimeoutError()])
async def test_connect_with_retry_retries_after_a_broker_error_then_connects(
    rabbitmq_url: str, fast_connect_retry: None, error: Exception
) -> None:
    rabbit_broker = RabbitBroker(rabbitmq_url)
    real_connect = rabbit_broker.connect
    attempts = 0

    async def connect_failing_once() -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise error
        return await real_connect()

    try:
        with patch.object(rabbit_broker, "connect", connect_failing_once):
            connected = await connect_with_retry(rabbit_broker, asyncio.Event())

        assert connected is True
        assert attempts == 2
        assert await (await rabbit_broker.declare_queue(payments_new_queue)).get(fail=False) is None
    finally:
        await rabbit_broker.stop()


async def test_connect_with_retry_retries_when_topology_declaration_fails(
    rabbitmq_url: str, fast_connect_retry: None
) -> None:
    rabbit_broker = RabbitBroker(rabbitmq_url)
    real_declare = rabbit_broker.declare_exchange
    attempts = 0

    async def declare_failing_once(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise AMQPError("channel closed")
        return await real_declare(*args, **kwargs)

    try:
        with patch.object(rabbit_broker, "declare_exchange", declare_failing_once):
            connected = await connect_with_retry(rabbit_broker, asyncio.Event())

        assert connected is True
        assert attempts >= 3
    finally:
        await rabbit_broker.stop()


async def wait_for_log(caplog: pytest.LogCaptureFixture, message: str) -> None:
    deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if message in caplog.text:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"{message!r} was not logged in time")


async def test_connect_with_retry_returns_false_without_connecting_when_already_stopped(
    rabbitmq_url: str,
) -> None:
    rabbit_broker = RabbitBroker(rabbitmq_url)
    stop = asyncio.Event()
    stop.set()

    with patch.object(rabbit_broker, "connect", side_effect=AssertionError("must not connect")):
        connected = await connect_with_retry(rabbit_broker, stop)

    assert connected is False


async def test_connect_with_retry_gives_up_when_stopped_while_broker_is_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rabbit_broker = RabbitBroker("amqp://guest:guest@127.0.0.1:1/")
    stop = asyncio.Event()
    task = asyncio.create_task(connect_with_retry(rabbit_broker, stop))

    await wait_for_log(caplog, "RabbitMQ is not available")
    stop.set()

    assert await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS) is False
    assert "RabbitMQ is not available; retrying in 2.0s" in caplog.text


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
async def test_run_relay_publishes_outbox_rows_and_stops_cleanly_on_signal(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    rabbitmq_url: str,
    caplog: pytest.LogCaptureFixture,
    signum: signal.Signals,
) -> None:
    [event_id] = await insert_outbox_events(session_factory, 1)
    relay_settings = settings.model_copy(
        update={"rabbitmq_url": rabbitmq_url, "relay_poll_interval_seconds": 0.05}
    )
    caplog.set_level(logging.INFO)
    task = asyncio.create_task(run_relay(relay_settings))

    await wait_until_all_published(session_factory)
    os.kill(os.getpid(), signum)
    await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert [m.message_id for m in await drain_queue(broker)] == [str(event_id)]
    assert "Outbox relay started" in caplog.text
    assert "Outbox relay stopped" in caplog.text


async def test_run_relay_stops_on_signal_while_rabbitmq_is_unreachable(
    settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    unreachable = settings.model_copy(update={"rabbitmq_url": "amqp://guest:guest@127.0.0.1:1/"})
    caplog.set_level(logging.INFO)
    task = asyncio.create_task(run_relay(unreachable))

    await wait_for_log(caplog, "RabbitMQ is not available")
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=WAIT_TIMEOUT_SECONDS)

    assert "Outbox relay started" not in caplog.text
    assert "Outbox relay stopped" in caplog.text


async def test_main_queue_is_declared_with_the_arguments_that_make_dead_lettering_at_least_once(
    broker: RabbitBroker, rabbitmq_url: str
) -> None:
    # The broker fixture declared the topology from the application code. RabbitMQ rejects a
    # re-declaration whose arguments differ, so a successful declaration with exactly these
    # arguments proves what the broker holds. Without x-overflow
    # reject-publish it would silently fall back to at-most-once dead-lettering.
    expected = {
        "x-queue-type": "quorum",
        "x-delivery-limit": 2,
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        matching_channel = await connection.channel()
        await matching_channel.declare_queue("payments.new", durable=True, arguments=expected)

        without_overflow = {k: v for k, v in expected.items() if k != "x-overflow"}
        mismatch_channel = await connection.channel()
        with pytest.raises(ChannelPreconditionFailed):
            await mismatch_channel.declare_queue(
                "payments.new", durable=True, arguments=without_overflow
            )
    finally:
        await connection.close()
