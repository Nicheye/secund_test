import asyncio
import contextlib
import logging
import signal
import uuid

from aiormq.exceptions import AMQPError
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.messaging.outbox_repository import (
    lock_unpublished_events,
    mark_published,
    record_publish_failure,
)
from app.messaging.topology import ROUTING_KEY, declare_topology, payments_exchange

logger = logging.getLogger(__name__)

CONNECT_RETRY_SECONDS = 2.0
BROKER_ERRORS = (AMQPError, ConnectionError, TimeoutError)
LAST_ERROR_MAX_LENGTH = 1000


def create_broker(rabbitmq_url: str) -> RabbitBroker:
    """Broker for the relay: an unroutable (returned) message raises instead of being ignored.

    `mandatory=True` only makes RabbitMQ return an unroutable message; without
    `on_return_raises` the client silently drops that return and the publish looks successful.
    """
    return RabbitBroker(rabbitmq_url, default_channel=Channel(on_return_raises=True))


class Relay:
    """Publishes outbox rows to RabbitMQ.

    Delivery is at-least-once: a row is marked published only after the broker confirmed the
    message, in the transaction that holds its row lock. A crash between the confirm and the
    commit leaves the row unpublished, so it is published again; the consumer must be idempotent
    (message_id is the outbox id, the same on every redelivery).
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        *,
        poll_interval_seconds: float,
        batch_size: int,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._poll_interval_seconds = poll_interval_seconds
        self._batch_size = batch_size

    async def publish_batch(self) -> int:
        """Publish one batch; returns how many rows were published and marked.

        If a publish fails, rows already confirmed in this batch are still marked and committed.
        The failed row stays unpublished with its attempt count raised and the error stored (in
        a separate short transaction, after the batch transaction released its lock), so it sorts
        behind healthy rows from then on; the error is then re-raised. Any exception counts, not
        only broker errors: an unserialisable payload is as permanent as a missing binding.
        """
        confirmed_ids: list[uuid.UUID] = []
        failure: tuple[uuid.UUID, Exception] | None = None
        async with self._session_factory() as session:
            for event in await lock_unpublished_events(session, self._batch_size):
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=payments_exchange,
                        routing_key=ROUTING_KEY,
                        message_id=str(event.id),
                        persist=True,
                        mandatory=True,
                    )
                except Exception as error:  # any failure demotes this row, see below
                    failure = (event.id, error)
                    break
                confirmed_ids.append(event.id)
            if confirmed_ids:
                await mark_published(session, confirmed_ids)
            await session.commit()
        if failure is not None:
            await self._record_failure(*failure)
            raise failure[1]
        return len(confirmed_ids)

    async def _record_failure(self, event_id: uuid.UUID, error: Exception) -> None:
        description = f"{type(error).__name__}: {error}"[:LAST_ERROR_MAX_LENGTH]
        async with self._session_factory() as session:
            await record_publish_failure(session, event_id, description)
            await session.commit()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                published = await self.publish_batch()
            except Exception:
                # Not only broker errors: a poison row must not crash-loop the relay.
                logger.exception("Outbox publish failed; unpublished rows will be retried")
                published = 0
            if published == self._batch_size:
                continue
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_interval_seconds)


async def connect_with_retry(broker: RabbitBroker, stop: asyncio.Event) -> bool:
    """Connect and declare topology, retrying until success; False if stopped first."""
    while not stop.is_set():
        try:
            await broker.connect()
            await declare_topology(broker)
        except BROKER_ERRORS:
            logger.exception("RabbitMQ is not available; retrying in %ss", CONNECT_RETRY_SECONDS)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=CONNECT_RETRY_SECONDS)
        else:
            return True
    return False


async def run_relay(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)

    engine = create_engine(settings.database_url)
    broker = create_broker(settings.rabbitmq_url)
    try:
        if await connect_with_retry(broker, stop):
            logger.info("Outbox relay started")
            relay = Relay(
                create_session_factory(engine),
                broker,
                poll_interval_seconds=settings.relay_poll_interval_seconds,
                batch_size=settings.relay_batch_size,
            )
            await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()
        logger.info("Outbox relay stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run_relay(get_settings()))
