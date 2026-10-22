import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.main import create_app
from app.config import Settings, get_settings


@pytest.fixture
def environment(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", settings.database_url)
    monkeypatch.setenv("RABBITMQ_URL", settings.rabbitmq_url)
    monkeypatch.setenv("API_KEY", settings.api_key)
    get_settings.cache_clear()


async def test_lifespan_builds_settings_and_database_access_from_environment(
    environment: None, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    app = create_app()

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/payments/22222222-2222-4222-8222-222222222222",
                headers={"X-API-Key": "test-key"},
            )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "payment_not_found"
    get_settings.cache_clear()


async def test_lifespan_rejects_wrong_api_key_taken_from_environment(
    environment: None, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    app = create_app()

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/payments/22222222-2222-4222-8222-222222222222",
                headers={"X-API-Key": "other"},
            )

    assert response.status_code == 401
    get_settings.cache_clear()


async def test_lifespan_keeps_injected_settings_and_session_factory(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    app = create_app(settings=settings, session_factory=session_factory)

    async with app.router.lifespan_context(app):
        assert app.state.settings is settings
        assert app.state.session_factory is session_factory

    assert app.state.session_factory is session_factory


def test_importing_the_module_needs_no_environment() -> None:
    from app.api.main import app

    assert app.title == "Payment service"
