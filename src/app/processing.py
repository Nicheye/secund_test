import logging
import uuid

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain import PaymentStatus, transition
from app.gateway import PaymentGateway
from app.models import Payment
from app.webhook import WebhookSender

logger = logging.getLogger(__name__)


async def complete_payment(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: uuid.UUID,
    gateway: PaymentGateway,
    webhook_sender: WebhookSender,
) -> None:
    """Runs the gateway for a pending payment, stores the outcome and notifies the client.

    Safe to run again for the same payment: a finished payment skips the gateway and only
    repeats the webhook. No transaction is open while the gateway call is in flight.
    """
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        # Redelivery cannot fix a missing row, so the message is acknowledged.
        logger.error("Payment not found, dropping message payment_id=%s", payment_id)
        return

    if payment.status == PaymentStatus.PENDING:
        outcome = await gateway.process(payment)
        payment = await _store_outcome(session_factory, payment, outcome)
    else:
        logger.info(
            "Payment already %s, skipping gateway payment_id=%s", payment.status, payment_id
        )

    await webhook_sender.send(payment)
    logger.info("Payment completed payment_id=%s status=%s", payment_id, payment.status)


async def _store_outcome(
    session_factory: async_sessionmaker[AsyncSession],
    payment: Payment,
    outcome: PaymentStatus,
) -> Payment:
    new_status = transition(PaymentStatus(payment.status), outcome)
    # Compare-and-set on status: of several workers racing on one payment only the first
    # write wins, and the others adopt its result instead of overwriting it.
    statement = (
        update(Payment)
        .where(Payment.id == payment.id, Payment.status == PaymentStatus.PENDING.value)
        .values(status=new_status.value, processed_at=func.now())
        .returning(Payment)
    )
    async with session_factory() as session:
        updated = (await session.execute(statement)).scalar_one_or_none()
        if updated is None:
            logger.info("Payment finished by another worker payment_id=%s", payment.id)
            updated = await session.get_one(Payment, payment.id, populate_existing=True)
        await session.commit()
    return updated
