from typing import Any

import pytest
from pydantic import ValidationError

from app.config import Settings, get_settings

REQUIRED: dict[str, str] = {
    "database_url": "postgresql+asyncpg://user:pass@db/payments",
    "rabbitmq_url": "amqp://guest:guest@mq/",
    "api_key": "secret",
}
ENVIRONMENT_NAMES = [name.upper() for name in Settings.model_fields]


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENVIRONMENT_NAMES:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()


def make_settings(**overrides: Any) -> Settings:
    return Settings(**{**REQUIRED, **overrides}, _env_file=None)


def test_settings_have_documented_defaults() -> None:
    settings = make_settings()

    assert settings.model_dump() == {
        **REQUIRED,
        "api_docs_enabled": False,
        "relay_poll_interval_seconds": 1.0,
        "relay_batch_size": 50,
        "consumer_prefetch_count": 10,
        "webhook_max_attempts": 3,
        "webhook_backoff_base_seconds": 1.0,
        "webhook_timeout_seconds": 5.0,
        "gateway_min_seconds": 2,
        "gateway_max_seconds": 5,
        "gateway_success_rate": 0.9,
    }


@pytest.mark.parametrize("missing", ["database_url", "rabbitmq_url", "api_key"])
def test_rejects_settings_when_required_value_is_missing(missing: str) -> None:
    values = {key: value for key, value in REQUIRED.items() if key != missing}

    with pytest.raises(ValidationError) as error:
        Settings(**values, _env_file=None)  # type: ignore[arg-type]

    assert [item["loc"] for item in error.value.errors()] == [(missing,)]
    assert error.value.errors()[0]["type"] == "missing"


def test_reads_values_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://env/db")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://env/")
    monkeypatch.setenv("API_KEY", "env-key")
    monkeypatch.setenv("WEBHOOK_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("RELAY_POLL_INTERVAL_SECONDS", "0.25")

    settings = Settings(_env_file=None)

    assert settings.database_url == "postgresql+asyncpg://env/db"
    assert settings.api_key == "env-key"
    assert settings.webhook_max_attempts == 5
    assert settings.relay_poll_interval_seconds == 0.25


def test_get_settings_reads_environment_once_and_returns_the_same_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", REQUIRED["database_url"])
    monkeypatch.setenv("RABBITMQ_URL", REQUIRED["rabbitmq_url"])
    monkeypatch.setenv("API_KEY", "first")
    first = get_settings()
    monkeypatch.setenv("API_KEY", "second")

    second = get_settings()

    assert first is second
    assert second.api_key == "first"


def test_settings_are_immutable() -> None:
    settings = make_settings()

    with pytest.raises(ValidationError):
        settings.api_key = "other"  # type: ignore[misc]


@pytest.mark.parametrize("attempts", [1, 3, 10])
def test_accepts_webhook_max_attempts_of_at_least_one(attempts: int) -> None:
    assert make_settings(webhook_max_attempts=attempts).webhook_max_attempts == attempts


@pytest.mark.parametrize("attempts", [0, -1])
def test_rejects_webhook_max_attempts_below_one(attempts: int) -> None:
    with pytest.raises(ValidationError, match="webhook_max_attempts"):
        make_settings(webhook_max_attempts=attempts)


@pytest.mark.parametrize("batch_size", [0, -5])
def test_rejects_relay_batch_size_below_one(batch_size: int) -> None:
    with pytest.raises(ValidationError, match="relay_batch_size"):
        make_settings(relay_batch_size=batch_size)


@pytest.mark.parametrize("rate", [0, 0.0, 0.5, 1, 1.0])
def test_accepts_gateway_success_rate_between_zero_and_one_inclusive(rate: float) -> None:
    assert make_settings(gateway_success_rate=rate).gateway_success_rate == rate


@pytest.mark.parametrize("rate", [-0.01, 1.01, 2, -1])
def test_rejects_gateway_success_rate_outside_zero_to_one(rate: float) -> None:
    with pytest.raises(ValidationError, match="gateway_success_rate"):
        make_settings(gateway_success_rate=rate)


@pytest.mark.parametrize(("minimum", "maximum"), [(0, 0), (2, 2), (2, 5), (0, 10)])
def test_accepts_gateway_delay_range_when_minimum_does_not_exceed_maximum(
    minimum: int, maximum: int
) -> None:
    settings = make_settings(gateway_min_seconds=minimum, gateway_max_seconds=maximum)

    assert (settings.gateway_min_seconds, settings.gateway_max_seconds) == (minimum, maximum)


@pytest.mark.parametrize(("minimum", "maximum"), [(6, 5), (1, 0), (3, 2)])
def test_rejects_gateway_delay_range_when_minimum_exceeds_maximum(
    minimum: int, maximum: int
) -> None:
    with pytest.raises(ValidationError, match="gateway_min_seconds must not exceed"):
        make_settings(gateway_min_seconds=minimum, gateway_max_seconds=maximum)


@pytest.mark.parametrize("count", [1, 10, 100])
def test_accepts_consumer_prefetch_count_of_at_least_one(count: int) -> None:
    assert make_settings(consumer_prefetch_count=count).consumer_prefetch_count == count


@pytest.mark.parametrize("count", [0, -1])
def test_rejects_consumer_prefetch_count_below_one(count: int) -> None:
    with pytest.raises(ValidationError, match="consumer_prefetch_count"):
        make_settings(consumer_prefetch_count=count)


def test_reads_consumer_prefetch_count_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONSUMER_PREFETCH_COUNT", "4")

    assert make_settings().consumer_prefetch_count == 4


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("false", False)])
def test_reads_api_docs_enabled_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool
) -> None:
    monkeypatch.setenv("API_DOCS_ENABLED", raw)

    assert make_settings().api_docs_enabled is expected


@pytest.mark.parametrize(
    "raw", ["1", "0", "yes", "no", "on", "off", "t", "y", "True", "TRUE", " true", ""]
)
def test_rejects_api_docs_enabled_spellings_other_than_true_or_false(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("API_DOCS_ENABLED", raw)

    with pytest.raises(ValidationError, match="api_docs_enabled"):
        make_settings()


@pytest.mark.parametrize("value", [True, False])
def test_accepts_api_docs_enabled_given_as_a_bool(value: bool) -> None:
    assert make_settings(api_docs_enabled=value).api_docs_enabled is value
