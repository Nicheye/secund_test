import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, Payment


def make_payment(**overrides: Any) -> Payment:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "amount": Decimal("10.00"),
        "currency": "USD",
        "description": "order",
        "status": "pending",
        "idempotency_key": f"key-{uuid.uuid4()}",
        "request_fingerprint": "fingerprint",
        "webhook_url": "https://client.example/hook",
    }
    return Payment(**{**values, **overrides})


async def commit(session_factory: async_sessionmaker[AsyncSession], *rows: Any) -> None:
    async with session_factory() as session:
        session.add_all(rows)
        await session.commit()


async def count(session_factory: async_sessionmaker[AsyncSession], table: str) -> int:
    async with session_factory() as session:
        result = await session.execute(text(f"SELECT count(*) FROM {table}"))
        return int(result.scalar_one())


async def test_database_accepts_a_valid_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await commit(session_factory, make_payment())

    assert await count(session_factory, "payments") == 1


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"amount": Decimal("0")}, "ck_payments_amount_positive"),
        ({"amount": Decimal("-0.01")}, "ck_payments_amount_positive"),
        ({"currency": "GBP"}, "ck_payments_currency_known"),
        ({"currency": "usd"}, "ck_payments_currency_known"),
        ({"currency": ""}, "ck_payments_currency_known"),
        ({"status": "unknown"}, "ck_payments_status_known"),
        ({"status": "PENDING"}, "ck_payments_status_known"),
        ({"status": ""}, "ck_payments_status_known"),
    ],
)
async def test_database_rejects_payment_violating_check_constraint(
    session_factory: async_sessionmaker[AsyncSession], overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        await commit(session_factory, make_payment(**overrides))

    assert await count(session_factory, "payments") == 0


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
@pytest.mark.parametrize("status", ["pending", "succeeded", "failed"])
async def test_database_accepts_every_known_currency_and_status(
    session_factory: async_sessionmaker[AsyncSession], currency: str, status: str
) -> None:
    await commit(session_factory, make_payment(currency=currency, status=status))

    assert await count(session_factory, "payments") == 1


@pytest.mark.parametrize(
    "column",
    [
        "amount",
        "currency",
        "description",
        "payment_metadata",
        "status",
        "idempotency_key",
        "request_fingerprint",
        "webhook_url",
    ],
)
async def test_database_rejects_payment_with_null_in_required_column(
    session_factory: async_sessionmaker[AsyncSession], column: str
) -> None:
    payment = make_payment()
    # Bypass ORM defaults: the column default for status/metadata would hide the NULL.
    values = {
        "id": uuid.uuid4(),
        "amount": Decimal("1.00"),
        "currency": "USD",
        "description": "d",
        "metadata": {},
        "status": "pending",
        "idempotency_key": payment.idempotency_key,
        "request_fingerprint": "f",
        "webhook_url": "u",
    }
    column_name = "metadata" if column == "payment_metadata" else column
    values[column_name] = None
    names = ", ".join(values)
    placeholders = ", ".join(
        f"CAST(:{name} AS jsonb)" if name == "metadata" else f":{name}" for name in values
    )
    statement = text(f"INSERT INTO payments ({names}) VALUES ({placeholders})")

    async with session_factory() as session:
        with pytest.raises(IntegrityError, match=r"null value in column"):
            await session.execute(statement, {**values, "metadata": None})


async def test_database_rejects_second_payment_with_same_idempotency_key(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await commit(session_factory, make_payment(idempotency_key="same"))

    with pytest.raises(IntegrityError, match="idempotency_key"):
        await commit(session_factory, make_payment(idempotency_key="same"))

    assert await count(session_factory, "payments") == 1


async def test_database_fills_defaults_for_metadata_status_and_created_at(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO payments (id, amount, currency, description, status, "
                "idempotency_key, request_fingerprint, webhook_url) VALUES "
                "(:id, 1, 'USD', 'd', 'pending', 'k', 'f', 'u')"
            ),
            {"id": uuid.UUID("11111111-1111-4111-8111-111111111111")},
        )
        await session.commit()
        stored = await session.get_one(Payment, uuid.UUID("11111111-1111-4111-8111-111111111111"))

    assert stored.payment_metadata == {}
    assert stored.processed_at is None
    assert stored.created_at.tzinfo is not None
    assert stored.created_at <= datetime.now(UTC)


async def test_orm_defaults_new_payment_to_pending_with_empty_metadata(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = Payment(
            amount=Decimal("1.00"),
            currency="USD",
            description="d",
            idempotency_key="k",
            request_fingerprint="f",
            webhook_url="u",
        )
        session.add(payment)
        await session.commit()
        stored = await session.get_one(Payment, payment.id, populate_existing=True)

    assert (stored.status, stored.payment_metadata) == ("pending", {})


async def test_database_stores_amount_with_two_decimal_places(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = make_payment(amount=Decimal("1000000000.00"))
    await commit(session_factory, payment)

    async with session_factory() as session:
        stored = await session.get_one(Payment, payment.id)

    assert str(stored.amount) == "1000000000.00"


async def test_database_rejects_outbox_event_for_unknown_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = OutboxEvent(aggregate_id=uuid.uuid4(), event_type="payment.created", payload={})

    with pytest.raises(IntegrityError, match="outbox_aggregate_id_fkey"):
        await commit(session_factory, event)

    assert await count(session_factory, "outbox") == 0


async def test_database_restricts_deleting_a_payment_that_has_outbox_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = make_payment()
    await commit(session_factory, payment)
    await commit(
        session_factory,
        OutboxEvent(aggregate_id=payment.id, event_type="payment.created", payload={}),
    )

    async with session_factory() as session:
        with pytest.raises(IntegrityError, match="outbox_aggregate_id_fkey"):
            await session.execute(text("DELETE FROM payments WHERE id = :id"), {"id": payment.id})

    assert await count(session_factory, "payments") == 1
    assert await count(session_factory, "outbox") == 1


async def test_outbox_event_defaults_to_unpublished_with_creation_time(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = make_payment()
    event = OutboxEvent(aggregate_id=payment.id, event_type="payment.created", payload={"k": "v"})
    await commit(session_factory, payment)
    await commit(session_factory, event)

    async with session_factory() as session:
        stored = await session.get_one(OutboxEvent, event.id)

    assert stored.published_at is None
    assert stored.created_at.tzinfo is not None
    assert stored.payload == {"k": "v"}


async def test_outbox_has_partial_index_on_unpublished_rows_only(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        definition = (
            await session.execute(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = :name"),
                {"name": "ix_outbox_unpublished_created_at"},
            )
        ).scalar_one()

    assert definition == (
        "CREATE INDEX ix_outbox_unpublished_created_at ON public.outbox "
        "USING btree (created_at) WHERE (published_at IS NULL)"
    )
