import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from app.api.main import create_app
from app.config import Settings
from app.db import create_engine, create_session_factory

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_API_KEY = "test-key"
TEST_RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
RABBITMQ_IMAGE = "rabbitmq:4.0-alpine"


def _run_migrations(database_url: str) -> None:
    environment = {
        **os.environ,
        "DATABASE_URL": database_url,
        "RABBITMQ_URL": TEST_RABBITMQ_URL,
        "API_KEY": TEST_API_KEY,
    }
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
    )


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    external_url = os.environ.get("TEST_DATABASE_URL")
    if external_url:
        _run_migrations(external_url)
        yield external_url
        return
    with PostgresContainer("postgres:16", driver="asyncpg") as container:
        url = container.get_connection_url()
        _run_migrations(url)
        yield url


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    with RabbitMqContainer(RABBITMQ_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    test_engine = create_engine(postgres_url)
    yield test_engine
    await test_engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE outbox, payments"))
    return create_session_factory(engine)


@pytest.fixture
def settings(postgres_url: str) -> Settings:
    return Settings(
        database_url=postgres_url,
        rabbitmq_url=TEST_RABBITMQ_URL,
        api_key=TEST_API_KEY,
        _env_file=None,
    )


@pytest.fixture
async def client(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings=settings, session_factory=session_factory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client
