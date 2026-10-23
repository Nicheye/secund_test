import importlib
from pathlib import Path

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.routing import Route

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


def test_importing_the_module_needs_no_environment(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for name in ("DATABASE_URL", "RABBITMQ_URL", "API_KEY", "API_DOCS_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    # Settings reads ".env" from the working directory: run where there is none.
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()
    import app.api.main as main_module

    try:
        reloaded = importlib.reload(main_module)

        # Importing must neither read settings nor build the app eagerly.
        assert get_settings.cache_info().currsize == 0
        assert "app" not in vars(reloaded)
        built = reloaded.create_app(settings=settings)
        assert built.title == "Payment service"
        assert built.state.settings is settings
    finally:
        get_settings.cache_clear()


def test_module_attribute_app_builds_the_api_from_environment(environment: None) -> None:
    import app.api.main as main_module

    try:
        assert main_module.app.title == "Payment service"
    finally:
        get_settings.cache_clear()


def test_unknown_module_attribute_raises_attribute_error() -> None:
    import app.api.main as main_module

    with pytest.raises(AttributeError, match="no_such_attribute"):
        main_module.no_such_attribute  # noqa: B018


DOCS_PATHS = ["/docs", "/redoc", "/openapi.json"]


@pytest.mark.parametrize("path", DOCS_PATHS)
async def test_docs_routes_do_not_exist_by_default(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession], path: str
) -> None:
    app = create_app(settings=settings, session_factory=session_factory)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(path)

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "not_found", "message": "Not Found"}}


@pytest.mark.parametrize("path", DOCS_PATHS)
async def test_docs_routes_exist_when_enabled(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession], path: str
) -> None:
    app = create_app(
        settings=settings.model_copy(update={"api_docs_enabled": True}),
        session_factory=session_factory,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(path)

    assert response.status_code == 200


@pytest.mark.parametrize("docs_enabled", [False, True])
async def test_every_route_except_the_docs_requires_the_api_key(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession], docs_enabled: bool
) -> None:
    app = create_app(
        settings=settings.model_copy(update={"api_docs_enabled": docs_enabled}),
        session_factory=session_factory,
    )
    # Routes added by include_router are only listed (lazily) through the OpenAPI schema, so
    # the plain routes of the app are checked to be nothing but the docs ones.
    plain_paths = {route.path for route in app.routes if isinstance(route, Route)}
    assert plain_paths == (set(DOCS_PATHS) if docs_enabled else set())
    operations = [
        (method.upper(), path) for path, item in app.openapi()["paths"].items() for method in item
    ]
    assert operations

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for method, path in operations:
            url = path.replace("{payment_id}", "22222222-2222-4222-8222-222222222222")
            response = await client.request(method, url)
            assert response.status_code == 401, (method, path)
            assert response.json()["error"]["code"] == "unauthorized"
