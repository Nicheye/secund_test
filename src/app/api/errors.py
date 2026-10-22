from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.schemas import ErrorResponse
from app.services import IdempotencyKeyConflict, PaymentNotFound

MAX_IDEMPOTENCY_KEY_LENGTH = 255


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def unauthorized() -> ApiError:
    return ApiError(status.HTTP_401_UNAUTHORIZED, "unauthorized", "Missing or invalid API key")


def missing_idempotency_key() -> ApiError:
    return ApiError(
        status.HTTP_400_BAD_REQUEST,
        "idempotency_key_required",
        "Idempotency-Key header is required",
    )


def invalid_idempotency_key() -> ApiError:
    return ApiError(
        status.HTTP_400_BAD_REQUEST,
        "idempotency_key_invalid",
        f"Idempotency-Key must be at most {MAX_IDEMPOTENCY_KEY_LENGTH} characters",
    )


def payment_not_found() -> ApiError:
    return ApiError(status.HTTP_404_NOT_FOUND, "payment_not_found", "Payment not found")


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    body = ErrorResponse.model_validate({"error": {"code": code, "message": message}})
    return JSONResponse(status_code=status_code, content=body.model_dump())


async def _handle_api_error(_: Request, error: Exception) -> JSONResponse:
    assert isinstance(error, ApiError)
    return _error_response(error.status_code, error.code, error.message)


async def _handle_idempotency_conflict(_: Request, __: Exception) -> JSONResponse:
    return _error_response(
        status.HTTP_409_CONFLICT,
        "idempotency_key_conflict",
        "Idempotency-Key was already used with a different request payload",
    )


async def _handle_payment_not_found(_: Request, __: Exception) -> JSONResponse:
    error = payment_not_found()
    return _error_response(error.status_code, error.code, error.message)


async def _handle_validation_error(_: Request, error: Exception) -> JSONResponse:
    assert isinstance(error, RequestValidationError)
    # Only location and message are reported; the rejected input is never echoed back.
    message = "; ".join(
        f"{'.'.join(str(part) for part in item['loc'])}: {item['msg']}" for item in error.errors()
    )
    return _error_response(status.HTTP_422_UNPROCESSABLE_CONTENT, "validation_error", message)


async def _handle_http_exception(_: Request, error: Exception) -> JSONResponse:
    assert isinstance(error, StarletteHTTPException)
    code = "not_found" if error.status_code == status.HTTP_404_NOT_FOUND else "http_error"
    return _error_response(error.status_code, code, str(error.detail))


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _handle_api_error)
    app.add_exception_handler(IdempotencyKeyConflict, _handle_idempotency_conflict)
    app.add_exception_handler(PaymentNotFound, _handle_payment_not_found)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(StarletteHTTPException, _handle_http_exception)


def documented_errors(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: {"model": ErrorResponse} for code in status_codes}
