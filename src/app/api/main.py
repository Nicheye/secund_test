from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.errors import register_error_handlers
from app.api.routes import router
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Anything not injected by create_app is built here, so importing this module needs no
    # environment. Only an engine created here is disposed here.
    if app.state.settings is None:
        app.state.settings = get_settings()
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
    app = FastAPI(title="Payment service", version="0.1.0", lifespan=_lifespan)
    app.state.settings = settings
    app.state.session_factory = session_factory
    register_error_handlers(app)
    app.include_router(router)
    return app


app = create_app()
