import uuid

from fastapi import APIRouter, Depends, status

from app import services
from app.api.deps import IdempotencyKeyDep, SessionDep, require_api_key
from app.api.errors import documented_errors, payment_not_found
from app.schemas import CreatePaymentRequest, PaymentCreatedResponse, PaymentResponse

router = APIRouter(prefix="/api/v1/payments", dependencies=[Depends(require_api_key)])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreatedResponse,
    responses=documented_errors(400, 401, 409, 422),
)
async def create_payment(
    body: CreatePaymentRequest, idempotency_key: IdempotencyKeyDep, session: SessionDep
) -> PaymentCreatedResponse:
    payment = await services.create_payment(session, body, idempotency_key)
    return PaymentCreatedResponse.from_payment(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    responses=documented_errors(401, 404),
)
async def get_payment(payment_id: str, session: SessionDep) -> PaymentResponse:
    try:
        parsed_id = uuid.UUID(payment_id)
    except ValueError:
        raise payment_not_found() from None
    payment = await services.get_payment(session, parsed_id)
    return PaymentResponse.from_payment(payment)
