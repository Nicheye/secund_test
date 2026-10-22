import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment


async def insert_payment_if_absent(session: AsyncSession, values: dict[str, Any]) -> Payment | None:
    """Atomic against concurrent duplicates: None means the idempotency key already exists."""
    statement = (
        insert(Payment)
        .values(**values)
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    payment: Payment | None = await session.scalar(statement)
    return payment


async def get_payment_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Payment | None:
    statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
    payment: Payment | None = await session.scalar(statement)
    return payment


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def insert_outbox_event(
    session: AsyncSession, aggregate_id: uuid.UUID, event_type: str, payload: dict[str, Any]
) -> None:
    session.add(OutboxEvent(aggregate_id=aggregate_id, event_type=event_type, payload=payload))
    await session.flush()
