import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from app.db import create_engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
async def empty_database_url(postgres_url: str, engine: AsyncEngine) -> AsyncIterator[str]:
    name = f"migration_{uuid.uuid4().hex}"
    async with engine.connect() as connection:
        autocommit = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await autocommit.execute(text(f'CREATE DATABASE "{name}"'))
    yield make_url(postgres_url).set(database=name).render_as_string(hide_password=False)
    async with engine.connect() as connection:
        autocommit = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await autocommit.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))


def alembic(database_url: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = {
        **os.environ,
        "DATABASE_URL": database_url,
        "RABBITMQ_URL": "amqp://unused",
        "API_KEY": "unused",
    }
    return subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )


async def list_tables(database_url: str) -> list[str]:
    migrated_engine = create_engine(database_url)
    try:
        async with migrated_engine.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
                )
            )
            return [row[0] for row in rows]
    finally:
        await migrated_engine.dispose()


async def list_indexes(database_url: str) -> list[str]:
    migrated_engine = create_engine(database_url)
    try:
        async with migrated_engine.connect() as connection:
            rows = await connection.execute(
                text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY 1")
            )
            return [row[0] for row in rows]
    finally:
        await migrated_engine.dispose()


async def test_upgrade_creates_both_tables_and_the_partial_index(empty_database_url: str) -> None:
    alembic(empty_database_url, "upgrade", "head")

    assert await list_tables(empty_database_url) == ["alembic_version", "outbox", "payments"]
    assert "ix_outbox_unpublished_order" in await list_indexes(empty_database_url)


async def test_upgrade_leaves_no_difference_between_models_and_schema(
    empty_database_url: str,
) -> None:
    alembic(empty_database_url, "upgrade", "head")

    result = alembic(empty_database_url, "check")

    assert "No new upgrade operations detected" in result.stdout + result.stderr


async def test_downgrade_removes_every_table_and_upgrade_restores_them(
    empty_database_url: str,
) -> None:
    alembic(empty_database_url, "upgrade", "head")

    alembic(empty_database_url, "downgrade", "base")
    after_downgrade = await list_tables(empty_database_url)
    indexes_after_downgrade = await list_indexes(empty_database_url)
    alembic(empty_database_url, "upgrade", "head")

    assert after_downgrade == ["alembic_version"]
    assert indexes_after_downgrade == ["alembic_version_pkc"]
    assert await list_tables(empty_database_url) == ["alembic_version", "outbox", "payments"]


async def test_upgrade_head_twice_is_a_no_op(empty_database_url: str) -> None:
    alembic(empty_database_url, "upgrade", "head")

    alembic(empty_database_url, "upgrade", "head")

    assert await list_tables(empty_database_url) == ["alembic_version", "outbox", "payments"]


def test_there_is_exactly_one_migration_head() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "heads"],
        cwd=PROJECT_ROOT,
        env={
            **os.environ,
            "DATABASE_URL": "postgresql+asyncpg://unused/unused",
            "RABBITMQ_URL": "amqp://unused",
            "API_KEY": "unused",
        },
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip().splitlines() == ["0002 (head)"]


async def test_upgrade_0002_keeps_existing_outbox_rows_with_zero_attempts_and_no_error(
    empty_database_url: str,
) -> None:
    alembic(empty_database_url, "upgrade", "0001")
    migrated_engine = create_engine(empty_database_url)
    try:
        async with migrated_engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO payments (id, amount, currency, description, status, "
                    "idempotency_key, request_fingerprint, webhook_url) VALUES "
                    "(gen_random_uuid(), 1, 'USD', 'd', 'pending', 'k', 'f', 'http://x')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO outbox (id, aggregate_id, event_type, payload) "
                    "SELECT gen_random_uuid(), id, 'payment.created', '{}' FROM payments"
                )
            )

        alembic(empty_database_url, "upgrade", "head")

        async with migrated_engine.connect() as connection:
            row = (
                await connection.execute(text("SELECT publish_attempts, last_error FROM outbox"))
            ).one()
    finally:
        await migrated_engine.dispose()
    assert tuple(row) == (0, None)


async def list_outbox_columns(database_url: str) -> list[str]:
    migrated_engine = create_engine(database_url)
    try:
        async with migrated_engine.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'outbox' ORDER BY 1"
                )
            )
            return [row[0] for row in rows]
    finally:
        await migrated_engine.dispose()


async def test_downgrade_0002_restores_the_old_outbox_schema_and_upgrade_reapplies_it(
    empty_database_url: str,
) -> None:
    alembic(empty_database_url, "upgrade", "head")

    alembic(empty_database_url, "downgrade", "0001")
    columns_after_downgrade = await list_outbox_columns(empty_database_url)
    indexes_after_downgrade = await list_indexes(empty_database_url)
    alembic(empty_database_url, "upgrade", "head")

    assert "publish_attempts" not in columns_after_downgrade
    assert "last_error" not in columns_after_downgrade
    assert "ix_outbox_unpublished_created_at" in indexes_after_downgrade
    assert "ix_outbox_unpublished_order" not in indexes_after_downgrade
    assert "publish_attempts" in await list_outbox_columns(empty_database_url)
    assert "ix_outbox_unpublished_order" in await list_indexes(empty_database_url)
