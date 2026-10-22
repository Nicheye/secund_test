import uuid
from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


async def lock_unpublished_events(session: AsyncSession, limit: int) -> Sequence[OutboxEvent]:
    """Oldest unpublished events, row-locked until the transaction ends.

    SKIP LOCKED makes concurrent relays take disjoint batches instead of blocking on or
    duplicating each other's rows.
    """
    result = await session.scalars(
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at, OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return result.all()


async def mark_published(session: AsyncSession, event_id: uuid.UUID) -> None:
    await session.execute(
        update(OutboxEvent).where(OutboxEvent.id == event_id).values(published_at=func.now())
    )
