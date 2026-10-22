import hashlib
import json
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app import repositories
from app.models import Payment
from app.schemas import CreatePaymentRequest

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyKeyConflict(Exception):
    """The idempotency key was already used with a different payload."""


class PaymentNotFound(Exception):
    pass


def compute_request_fingerprint(request: CreatePaymentRequest) -> str:
    canonical = {
        # normalize() makes 10.5 and 10.50 the same payload; "f" avoids exponent notation.
        "amount": format(request.amount.normalize(), "f"),
        "currency": request.currency.value,
        "description": request.description,
        "metadata": request.metadata,
        "webhook_url": str(request.webhook_url),
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, request: CreatePaymentRequest, idempotency_key: str
) -> Payment:
    fingerprint = compute_request_fingerprint(request)
    values: dict[str, Any] = {
        "amount": request.amount,
        "currency": request.currency.value,
        "description": request.description,
        "payment_metadata": request.metadata,
        "idempotency_key": idempotency_key,
        "request_fingerprint": fingerprint,
        "webhook_url": str(request.webhook_url),
    }

    # The unique constraint decides the winner of concurrent duplicates; there is no
    # check-then-insert window.
    payment = await repositories.insert_payment_if_absent(session, values)
    if payment is not None:
        await repositories.insert_outbox_event(
            session, payment.id, PAYMENT_CREATED_EVENT, {"payment_id": str(payment.id)}
        )
        await session.commit()
        return payment

    existing = await repositories.get_payment_by_idempotency_key(session, idempotency_key)
    if existing is None:
        # Unreachable in normal operation: payments are never deleted, so a row that blocked
        # the insert is still there when it is read back.
        raise RuntimeError("Payment vanished after an idempotency key conflict")
    if existing.request_fingerprint != fingerprint:
        raise IdempotencyKeyConflict(idempotency_key)
    return existing


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await repositories.get_payment(session, payment_id)
    if payment is None:
        raise PaymentNotFound(str(payment_id))
    return payment
