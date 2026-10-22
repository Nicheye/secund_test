import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import UTC
from typing import Protocol

import httpx

from app.config import Settings
from app.domain import PaymentSnapshot
from app.timing import Sleeper

logger = logging.getLogger(__name__)

# httpx logs every request at INFO with the full URL, query string included, and the query may
# carry secrets (tokens). Only warnings and errors from httpx are kept.
logging.getLogger("httpx").setLevel(logging.WARNING)

RETRYABLE_STATUS_CODES = frozenset({408, 429})
SERVER_ERROR_MIN_STATUS_CODE = 500


class WebhookDeliveryError(Exception):
    pass


@dataclass(frozen=True)
class PostFailure:
    retryable: bool
    description: str


class WebhookSender(Protocol):
    async def send(self, payment: PaymentSnapshot) -> None: ...


def _loggable_url(url: str) -> str:
    # The query string may carry secrets (tokens), so it never reaches the logs.
    parsed = httpx.URL(url)
    return f"{parsed.scheme}://{parsed.host}{parsed.path}"


def _is_retryable(status_code: int) -> bool:
    return status_code >= SERVER_ERROR_MIN_STATUS_CODE or status_code in RETRYABLE_STATUS_CODES


class HttpWebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: Settings,
        sleeper: Sleeper = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._client = client
        self._max_attempts = settings.webhook_max_attempts
        self._backoff_base_seconds = settings.webhook_backoff_base_seconds
        self._timeout_seconds = settings.webhook_timeout_seconds
        self._sleeper = sleeper
        self._rng = rng or random.Random()

    async def send(self, payment: PaymentSnapshot) -> None:
        if payment.processed_at is None:
            raise ValueError("Webhook can only be sent for a processed payment")
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "processed_at": payment.processed_at.astimezone(UTC).isoformat(),
        }
        url = _loggable_url(payment.webhook_url)

        for attempt in range(1, self._max_attempts + 1):
            failure = await self._post(payment.webhook_url, payload)
            if failure is None:
                return
            logger.warning(
                "Webhook attempt %d/%d failed payment_id=%s url=%s reason=%s",
                attempt,
                self._max_attempts,
                payment.id,
                url,
                failure.description,
            )
            if not failure.retryable:
                raise WebhookDeliveryError(f"Webhook rejected: {failure.description}")
            if attempt == self._max_attempts:
                raise WebhookDeliveryError(
                    f"Webhook not delivered after {self._max_attempts} attempts: "
                    f"{failure.description}"
                )
            await self._sleeper(self._backoff_delay(attempt))

    async def _post(self, url: str, payload: dict[str, str]) -> PostFailure | None:
        """Returns None on success, otherwise why the attempt failed."""
        try:
            response = await self._client.post(url, json=payload, timeout=self._timeout_seconds)
        except httpx.TransportError as error:
            return PostFailure(retryable=True, description=type(error).__name__)
        if response.is_success:
            return None
        return PostFailure(
            retryable=_is_retryable(response.status_code),
            description=f"HTTP {response.status_code}",
        )

    def _backoff_delay(self, attempt: int) -> float:
        exponential = self._backoff_base_seconds * 2.0 ** (attempt - 1)
        return exponential + self._rng.uniform(0, self._backoff_base_seconds)
