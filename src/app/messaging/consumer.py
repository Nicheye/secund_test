import asyncio
import logging
import uuid
from typing import Any

import httpx
from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import Channel, RabbitBroker
from pydantic import BaseModel

from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.gateway import EmulatedGateway, PaymentGateway
from app.messaging.topology import declare_topology, payments_exchange, payments_new_queue
from app.payment_finalization import finalize_payment
from app.webhook import HttpWebhookSender, WebhookSender

logger = logging.getLogger(__name__)


class PaymentNewMessage(BaseModel):
    payment_id: uuid.UUID


def create_app(
    settings: Settings,
    gateway: PaymentGateway | None = None,
    webhook_sender: WebhookSender | None = None,
) -> FastStream:
    """Builds the consumer. The gateway and webhook sender can be replaced for tests."""
    broker = RabbitBroker(settings.rabbitmq_url)
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)
    gateway = gateway or EmulatedGateway(settings)
    webhook_sender = webhook_sender or HttpWebhookSender(http_client, settings)

    # NACK_ON_ERROR requeues the message on any exception. On the quorum queue each requeue
    # counts toward x-delivery-limit, and after the limit RabbitMQ dead-letters the message.
    # The prefetch count is the consumer's concurrency: RabbitMQ sends at most this many
    # unacknowledged messages, and each one is handled in its own task.
    @broker.subscriber(
        payments_new_queue,
        payments_exchange,
        channel=Channel(prefetch_count=settings.consumer_prefetch_count),
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def consume_new_payment(message: PaymentNewMessage) -> None:
        logger.info("Message received payment_id=%s", message.payment_id)
        await finalize_payment(session_factory, message.payment_id, gateway, webhook_sender)

    async def declare_dead_letter_topology() -> None:
        await declare_topology(broker)

    async def release_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(
        broker,
        after_startup=[declare_dead_letter_topology],
        after_shutdown=[release_resources],
    )


def __getattr__(name: str) -> Any:
    # Lets `faststream run app.messaging.consumer:app` find `app` without reading settings
    # (and so requiring environment variables) just by importing this module.
    if name == "app":
        return create_app(get_settings())
    raise AttributeError(name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(create_app(get_settings()).run())
