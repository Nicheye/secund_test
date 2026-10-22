import uuid
from collections.abc import Sequence

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


async def lock_unpublished_events(session: AsyncSession, limit: int) -> Sequence[OutboxEvent]:
    """Unpublished events, row-locked until the transaction ends.

    Rows that failed to publish before come last, so one row that can never be published does
    not block the healthy rows behind it. SKIP LOCKED makes concurrent relays take disjoint
    batches instead of blocking on or duplicating each other's rows.
    """
    result = await session.scalars(
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.publish_attempts, OutboxEvent.created_at, OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return result.all()


async def mark_published(session: AsyncSession, event_ids: Sequence[uuid.UUID]) -> None:
    """Marks all given events published with one statement."""
    ids = bindparam("ids", list(event_ids), type_=ARRAY(UUID(as_uuid=True)))
    await session.execute(
        update(OutboxEvent).where(OutboxEvent.id == func.any(ids)).values(published_at=func.now())
    )


async def record_publish_failure(session: AsyncSession, event_id: uuid.UUID, error: str) -> None:
    """Counts a failed publish; runs in its own short transaction after the batch commit."""
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id == event_id)
        .values(publish_attempts=OutboxEvent.publish_attempts + 1, last_error=error)
    )
