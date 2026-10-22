import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.errors import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    invalid_idempotency_key,
    missing_idempotency_key,
    unauthorized,
)
from app.config import Settings


async def require_api_key(
    request: Request, x_api_key: Annotated[str | None, Header()] = None
) -> None:
    settings: Settings = request.app.state.settings
    if x_api_key is None or not secrets.compare_digest(
        x_api_key.encode(), settings.api_key.encode()
    ):
        raise unauthorized()


async def require_idempotency_key(
    idempotency_key: Annotated[str | None, Header()] = None,
) -> str:
    if not idempotency_key:
        raise missing_idempotency_key()
    if len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise invalid_idempotency_key()
    return idempotency_key


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKeyDep = Annotated[str, Depends(require_idempotency_key)]
