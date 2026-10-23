from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

ROUTING_KEY = "payments.new"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)

# Fanout: dead-lettered messages keep their original routing key, which the DLQ would
# otherwise have to be bound to.
payments_dlx = RabbitExchange("payments.dlx", type=ExchangeType.FANOUT, durable=True)

# How many times RabbitMQ delivers a message before dead-lettering it after errors other than a
# webhook failure (a failed webhook is rejected right away, see the consumer). This is a queue
# argument, deliberately not a setting and separate from WEBHOOK_MAX_ATTEMPTS (webhook POSTs).
# Changing queue arguments on an existing queue fails with PRECONDITION_FAILED, so changing
# this needs a new queue (or deleting `payments.new`).
QUEUE_MAX_DELIVERY_ATTEMPTS = 3

# Quorum queue: x-delivery-limit counts redeliveries, not deliveries, so the limit is one less
# than the attempts: the message is dead-lettered when attempt 3 is rejected
# (verified against RabbitMQ in tests/test_relay.py).
# "at-least-once" dead-lettering keeps the message in the queue until the DLX has confirmed it.
# RabbitMQ only honours it together with `x-overflow: reject-publish`; otherwise it logs a warning
# and silently falls back to at-most-once (verified on rabbitmq:4.0). The queue has no
# max-length, so the overflow strategy never triggers; it is set only to enable the strategy.
payments_new_queue = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=ROUTING_KEY,
    arguments={
        "x-delivery-limit": QUEUE_MAX_DELIVERY_ATTEMPTS - 1,
        "x-dead-letter-exchange": payments_dlx.name,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)

payments_dlq = RabbitQueue("payments.new.dlq", durable=True)


async def declare_topology(broker: RabbitBroker) -> None:
    """Declare exchanges, queues and bindings; safe to call repeatedly and from many processes."""
    exchange = await broker.declare_exchange(payments_exchange)
    dead_letter_exchange = await broker.declare_exchange(payments_dlx)
    queue = await broker.declare_queue(payments_new_queue)
    dead_letter_queue = await broker.declare_queue(payments_dlq)
    await queue.bind(exchange, routing_key=ROUTING_KEY)
    await dead_letter_queue.bind(dead_letter_exchange)
