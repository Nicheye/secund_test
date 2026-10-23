from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.errors import register_error_handlers
from app.api.routes import router
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    # The session factory is built here when create_app was not given one; only an engine
    # created here is disposed here.
    engine = None
    if app.state.session_factory is None:
        engine = create_engine(app.state.settings.database_url)
        app.state.session_factory = create_session_factory(engine)
    yield
    if engine is not None:
        await engine.dispose()


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    # The docs routes are the only ones that would need no X-API-Key, so they exist only when
    # explicitly enabled (local development).
    docs_url, redoc_url, openapi_url = (
        ("/docs", "/redoc", "/openapi.json") if settings.api_docs_enabled else (None, None, None)
    )
    app = FastAPI(
        title="Payment service",
        version="0.1.0",
        lifespan=_lifespan,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        swagger_ui_oauth2_redirect_url=None,
    )
    app.state.settings = settings
    app.state.session_factory = session_factory
    register_error_handlers(app)
    app.include_router(router)
    return app


def __getattr__(name: str) -> Any:
    # Lets `uvicorn app.api.main:app` find `app` without reading settings (and so requiring
    # environment variables) just by importing this module.
    if name == "app":
        return create_app()
    raise AttributeError(name)
