import json
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest

from app.config import Settings
from app.models import Payment
from app.webhook import HttpWebhookSender, WebhookDeliveryError

WEBHOOK_URL = "https://client.example/hook"


class FixedFractionRandom(random.Random):
    """uniform(a, b) always returns a + fraction * (b - a)."""

    def __init__(self, fraction: float) -> None:
        super().__init__()
        self._fraction = fraction

    def random(self) -> float:
        return self._fraction


class SleepRecorder:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


@pytest.fixture
def webhook_settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused",
        rabbitmq_url="amqp://unused",
        api_key="unused",
        _env_file=None,
    )


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        amount=Decimal("10.50"),
        currency="USD",
        description="order 1",
        status="succeeded",
        idempotency_key="key-1",
        request_fingerprint="fingerprint",
        webhook_url=WEBHOOK_URL,
        processed_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


def make_sender(
    settings: Settings,
    responses: list[int | Exception],
    sleeper: SleepRecorder,
    requests: list[httpx.Request],
) -> HttpWebhookSender:
    remaining = list(responses)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    return HttpWebhookSender(client, settings, sleeper=sleeper, rng=FixedFractionRandom(0.0))


async def test_posts_payment_result_as_json(webhook_settings: Settings, payment: Payment) -> None:
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [200], SleepRecorder(), requests)

    await sender.send(payment)

    assert len(requests) == 1
    assert str(requests[0].url) == WEBHOOK_URL
    assert json.loads(requests[0].content) == {
        "payment_id": "11111111-1111-1111-1111-111111111111",
        "status": "succeeded",
        "amount": "10.50",
        "currency": "USD",
        "processed_at": "2026-01-02T03:04:05+00:00",
    }


async def test_retries_after_500_and_succeeds_on_second_attempt(
    webhook_settings: Settings, payment: Payment
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [500, 200], sleeper, requests)

    await sender.send(payment)

    assert len(requests) == 2
    assert sleeper.delays == [1.0]


@pytest.mark.parametrize("status_code", [408, 429, 502, 503])
async def test_retries_on_retryable_status_codes(
    webhook_settings: Settings, payment: Payment, status_code: int
) -> None:
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [status_code, 204], SleepRecorder(), requests)

    await sender.send(payment)

    assert len(requests) == 2


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectTimeout("timed out"), httpx.ConnectError("refused"), httpx.ReadError("reset")],
)
async def test_retries_on_network_errors_and_timeouts(
    webhook_settings: Settings, payment: Payment, error: Exception
) -> None:
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [error, 200], SleepRecorder(), requests)

    await sender.send(payment)

    assert len(requests) == 2


async def test_backoff_doubles_between_attempts_and_adds_jitter(
    webhook_settings: Settings, payment: Payment
) -> None:
    sleeper = SleepRecorder()
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503)),
    )
    sender = HttpWebhookSender(
        client, webhook_settings, sleeper=sleeper, rng=FixedFractionRandom(0.5)
    )

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert sleeper.delays == [1.5, 2.5]


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 410, 422])
async def test_does_not_retry_non_retryable_client_errors(
    webhook_settings: Settings, payment: Payment, status_code: int
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [status_code], sleeper, requests)

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert len(requests) == 1
    assert sleeper.delays == []


async def test_raises_delivery_error_after_three_failed_attempts(
    webhook_settings: Settings, payment: Payment
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [500, 500, 500], sleeper, requests)

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert len(requests) == 3
    assert sleeper.delays == [1.0, 2.0]


async def test_does_not_log_webhook_url_query_string(
    webhook_settings: Settings, payment: Payment, caplog: pytest.LogCaptureFixture
) -> None:
    payment.webhook_url = f"{WEBHOOK_URL}?token=super-secret"
    sender = make_sender(webhook_settings, [500, 200], SleepRecorder(), [])

    with caplog.at_level(logging.WARNING):
        await sender.send(payment)

    assert "client.example/hook" in caplog.text
    assert "super-secret" not in caplog.text


async def test_refuses_to_send_payment_without_processed_at(
    webhook_settings: Settings, payment: Payment
) -> None:
    payment.processed_at = None
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [200], SleepRecorder(), requests)

    with pytest.raises(ValueError, match="processed"):
        await sender.send(payment)

    assert requests == []


def settings_with(**overrides: float) -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused",
        rabbitmq_url="amqp://unused",
        api_key="unused",
        _env_file=None,
        **overrides,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("status_code", [200, 201, 202, 204, 299])
async def test_treats_every_2xx_response_as_delivered_without_retry(
    webhook_settings: Settings, payment: Payment, status_code: int
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [status_code], sleeper, requests)

    await sender.send(payment)

    assert len(requests) == 1
    assert sleeper.delays == []


async def test_posts_json_with_post_method_and_json_content_type(
    webhook_settings: Settings, payment: Payment
) -> None:
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [200], SleepRecorder(), requests)

    await sender.send(payment)

    assert requests[0].method == "POST"
    assert requests[0].headers["content-type"] == "application/json"


async def test_posts_failed_rub_payment_with_exact_payload(webhook_settings: Settings) -> None:
    failed = Payment(
        id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        amount=Decimal("0.01"),
        currency="RUB",
        status="failed",
        webhook_url=WEBHOOK_URL,
        processed_at=datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC),
    )
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [200], SleepRecorder(), requests)

    await sender.send(failed)

    assert json.loads(requests[0].content) == {
        "payment_id": "22222222-2222-4222-8222-222222222222",
        "status": "failed",
        "amount": "0.01",
        "currency": "RUB",
        "processed_at": "2026-12-31T23:59:59+00:00",
    }


async def test_converts_processed_at_to_utc_in_payload(
    webhook_settings: Settings, payment: Payment
) -> None:
    payment.processed_at = datetime(2026, 1, 2, 6, 4, 5, tzinfo=timezone(timedelta(hours=3)))
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [200], SleepRecorder(), requests)

    await sender.send(payment)

    assert json.loads(requests[0].content)["processed_at"] == "2026-01-02T03:04:05+00:00"


async def test_sends_the_request_timeout_from_settings(payment: Payment) -> None:
    requests: list[httpx.Request] = []
    sender = make_sender(
        settings_with(webhook_timeout_seconds=7.5), [200], SleepRecorder(), requests
    )

    await sender.send(payment)

    assert requests[0].extensions["timeout"] == {
        "connect": 7.5,
        "read": 7.5,
        "write": 7.5,
        "pool": 7.5,
    }


@pytest.mark.parametrize("status_code", [500, 502, 503, 504, 599, 408, 429])
async def test_fails_after_three_attempts_for_each_retryable_status(
    webhook_settings: Settings, payment: Payment, status_code: int
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [status_code] * 3, sleeper, requests)

    with pytest.raises(WebhookDeliveryError) as error:
        await sender.send(payment)

    assert str(error.value) == f"Webhook not delivered after 3 attempts: HTTP {status_code}"
    assert len(requests) == 3
    assert sleeper.delays == [1.0, 2.0]


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectTimeout("t"),
        httpx.ReadTimeout("t"),
        httpx.WriteTimeout("t"),
        httpx.PoolTimeout("t"),
        httpx.ConnectError("c"),
        httpx.ReadError("r"),
        httpx.RemoteProtocolError("p"),
    ],
)
async def test_fails_after_three_attempts_when_transport_keeps_failing(
    webhook_settings: Settings, payment: Payment, error: httpx.TransportError
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [error] * 3, sleeper, requests)

    with pytest.raises(WebhookDeliveryError) as raised:
        await sender.send(payment)

    assert str(raised.value) == (f"Webhook not delivered after 3 attempts: {type(error).__name__}")
    assert len(requests) == 3
    assert sleeper.delays == [1.0, 2.0]


@pytest.mark.parametrize(
    "responses",
    [[500, 200], [503, 502, 204], [httpx.ReadTimeout("t"), 429, 201]],
)
async def test_succeeds_on_the_second_or_third_attempt_after_retryable_failures(
    webhook_settings: Settings, payment: Payment, responses: list[int | Exception]
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, responses, sleeper, requests)

    await sender.send(payment)

    assert len(requests) == len(responses)
    assert len(sleeper.delays) == len(responses) - 1


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 410, 301, 302, 100])
async def test_rejects_immediately_with_the_status_in_the_message_for_non_retryable_codes(
    webhook_settings: Settings, payment: Payment, status_code: int
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [status_code, 200], sleeper, requests)

    with pytest.raises(WebhookDeliveryError) as error:
        await sender.send(payment)

    assert str(error.value) == f"Webhook rejected: HTTP {status_code}"
    assert len(requests) == 1
    assert sleeper.delays == []


async def test_does_not_retry_after_a_retryable_failure_turns_into_a_client_error(
    webhook_settings: Settings, payment: Payment
) -> None:
    requests: list[httpx.Request] = []
    sleeper = SleepRecorder()
    sender = make_sender(webhook_settings, [503, 404, 200], sleeper, requests)

    with pytest.raises(WebhookDeliveryError, match="rejected: HTTP 404"):
        await sender.send(payment)

    assert len(requests) == 2
    assert sleeper.delays == [1.0]


@pytest.mark.parametrize(
    ("fraction", "expected_delays"),
    [(0.0, [1.0, 2.0, 4.0]), (0.5, [1.5, 2.5, 4.5]), (1.0, [2.0, 3.0, 5.0])],
)
async def test_backoff_is_base_times_two_to_the_attempt_plus_jitter_up_to_one_base(
    payment: Payment, fraction: float, expected_delays: list[float]
) -> None:
    sleeper = SleepRecorder()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    sender = HttpWebhookSender(
        client,
        settings_with(webhook_max_attempts=4),
        sleeper=sleeper,
        rng=FixedFractionRandom(fraction),
    )

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert sleeper.delays == expected_delays


async def test_backoff_uses_the_configured_base_for_exponent_and_jitter(payment: Payment) -> None:
    sleeper = SleepRecorder()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    sender = HttpWebhookSender(
        client,
        settings_with(webhook_backoff_base_seconds=0.5),
        sleeper=sleeper,
        rng=FixedFractionRandom(0.5),
    )

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert sleeper.delays == [0.75, 1.25]


async def test_backoff_jitter_comes_from_the_injected_seeded_generator(payment: Payment) -> None:
    sleeper = SleepRecorder()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    sender = HttpWebhookSender(client, settings_with(), sleeper=sleeper, rng=random.Random(2024))
    expected_jitter = random.Random(2024)

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert sleeper.delays == [
        1.0 + expected_jitter.uniform(0, 1.0),
        2.0 + expected_jitter.uniform(0, 1.0),
    ]
    assert all(1.0 <= delay <= 3.0 for delay in sleeper.delays)


async def test_does_not_sleep_after_the_last_attempt(payment: Payment) -> None:
    sleeper = SleepRecorder()
    requests: list[httpx.Request] = []
    sender = make_sender(settings_with(webhook_max_attempts=1), [500, 500], sleeper, requests)

    with pytest.raises(WebhookDeliveryError, match="after 1 attempts"):
        await sender.send(payment)

    assert len(requests) == 1
    assert sleeper.delays == []


async def test_makes_as_many_attempts_as_configured(payment: Payment) -> None:
    requests: list[httpx.Request] = []
    sender = make_sender(
        settings_with(webhook_max_attempts=5), [500] * 5, SleepRecorder(), requests
    )

    with pytest.raises(WebhookDeliveryError):
        await sender.send(payment)

    assert len(requests) == 5


async def test_logs_one_warning_per_failed_attempt_with_attempt_number_and_reason(
    webhook_settings: Settings, payment: Payment, caplog: pytest.LogCaptureFixture
) -> None:
    sender = make_sender(webhook_settings, [500, httpx.ConnectError("x"), 200], SleepRecorder(), [])

    with caplog.at_level(logging.WARNING):
        await sender.send(payment)

    assert [record.getMessage() for record in caplog.records] == [
        "Webhook attempt 1/3 failed payment_id=11111111-1111-1111-1111-111111111111 "
        "url=https://client.example/hook reason=HTTP 500",
        "Webhook attempt 2/3 failed payment_id=11111111-1111-1111-1111-111111111111 "
        "url=https://client.example/hook reason=ConnectError",
    ]


async def test_does_not_log_url_query_on_a_non_retryable_failure(
    webhook_settings: Settings, payment: Payment, caplog: pytest.LogCaptureFixture
) -> None:
    payment.webhook_url = f"{WEBHOOK_URL}?token=super-secret#frag"
    sender = make_sender(webhook_settings, [404], SleepRecorder(), [])

    with caplog.at_level(logging.DEBUG), pytest.raises(WebhookDeliveryError) as error:
        await sender.send(payment)

    assert "super-secret" not in caplog.text
    assert "super-secret" not in str(error.value)


async def test_still_posts_to_the_full_url_including_the_query(
    webhook_settings: Settings, payment: Payment
) -> None:
    payment.webhook_url = f"{WEBHOOK_URL}?token=abc"
    requests: list[httpx.Request] = []
    sender = make_sender(webhook_settings, [200], SleepRecorder(), requests)

    await sender.send(payment)

    assert str(requests[0].url) == f"{WEBHOOK_URL}?token=abc"
