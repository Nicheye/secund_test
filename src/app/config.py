from functools import lru_cache
from typing import Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """The only place environment variables are read."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=True)

    database_url: str
    rabbitmq_url: str
    api_key: str
    # Off by default: the docs routes are the only ones that need no X-API-Key.
    api_docs_enabled: bool = False
    relay_poll_interval_seconds: float = 1.0
    relay_batch_size: int = Field(default=50, ge=1)
    consumer_prefetch_count: int = Field(default=10, ge=1)
    webhook_max_attempts: int = Field(default=3, ge=1)
    webhook_backoff_base_seconds: float = 1.0
    webhook_timeout_seconds: float = 5.0
    gateway_min_seconds: int = 2
    gateway_max_seconds: int = 5
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    @field_validator("api_docs_enabled", mode="before")
    @classmethod
    def validate_api_docs_enabled_spelling(cls, value: object) -> object:
        # Only the exact strings "true" and "false" (or a real bool): the demo and run.sh read the
        # same variable and must never disagree with the API about whether docs are on.
        if isinstance(value, str) and value not in ("true", "false"):
            raise ValueError("api_docs_enabled must be exactly 'true' or 'false'")
        return value

    @model_validator(mode="after")
    def validate_gateway_delay_range(self) -> Self:
        if self.gateway_min_seconds > self.gateway_max_seconds:
            raise ValueError("gateway_min_seconds must not exceed gateway_max_seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
