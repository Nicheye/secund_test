import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Protocol

from app.config import Settings
from app.domain import PaymentStatus
from app.models import Payment

Sleeper = Callable[[float], Awaitable[None]]


class PaymentGateway(Protocol):
    async def process(self, payment: Payment) -> PaymentStatus: ...


class EmulatedGateway:
    """Stands in for an external payment provider: slow, and sometimes declines."""

    def __init__(
        self,
        settings: Settings,
        sleeper: Sleeper = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._min_seconds = settings.gateway_min_seconds
        self._max_seconds = settings.gateway_max_seconds
        self._success_rate = settings.gateway_success_rate
        self._sleeper = sleeper
        self._rng = rng or random.Random()

    async def process(self, payment: Payment) -> PaymentStatus:
        await self._sleeper(self._rng.uniform(self._min_seconds, self._max_seconds))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
