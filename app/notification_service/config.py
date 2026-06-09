"""Application configuration, loaded from environment / .env via pydantic-settings.

All hosts, ports, credentials, and tunables come from env. The async and sync
database URLs are derived from the discrete Postgres settings unless explicitly
overridden, so a single set of POSTGRES_* vars drives both the app (asyncpg) and
Alembic (psycopg).
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProviderMode(StrEnum):
    always_deliver = "always_deliver"
    always_reject = "always_reject"
    transient_then_deliver = "transient_then_deliver"
    random = "random"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Postgres ---
    postgres_user: str = "notify"
    postgres_password: str = "notify"
    postgres_db: str = "notifications"
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    database_url: str | None = None
    alembic_database_url: str | None = None

    # --- RabbitMQ ---
    rabbitmq_default_user: str = "notify"
    rabbitmq_default_pass: str = "notify"
    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    amqp_url: str | None = None

    # --- Redis ---
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_url: str | None = None

    # --- Provider ---
    provider_mode: ProviderMode = ProviderMode.random
    provider_receipt_delay: float = 0.2

    # --- Retry ---
    max_retries: int = 3
    # Fixed-delay retry tier TTLs (milliseconds). Defaults match the tier names
    # 5s / 30s / 120s; tests shrink them so the retry cycle runs in milliseconds.
    retry_ttl_5s_ms: int = 5_000
    retry_ttl_30s_ms: int = 30_000
    retry_ttl_120s_ms: int = 120_000

    # --- Rate limiting ---
    rate_limit_sms_per_sec: int = Field(default=50, ge=1)
    rate_limit_email_per_sec: int = Field(default=100, ge=1)

    # --- Idempotency ---
    idempotency_ttl_seconds: int = 86_400

    # --- Request limits ---
    # Maximum recipients accepted per POST /notifications (after de-duplication).
    max_recipients: int = Field(default=1000, ge=1)

    # --- Reconciler (sweeper) ---
    # How often the reconciler runs, and how old a row must be before it is
    # considered stuck. Thresholds must exceed normal processing + retry latency
    # so live work is never redriven prematurely (the CAS gate makes a redundant
    # redrive harmless either way).
    sweeper_interval_seconds: int = Field(default=30, ge=1)
    sweeper_queued_seconds: int = Field(default=60, ge=0)
    sweeper_sent_seconds: int = Field(default=120, ge=0)
    sweeper_batch_size: int = Field(default=100, ge=1)

    # --- Misc ---
    log_level: str = "INFO"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def effective_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def effective_alembic_url(self) -> str:
        if self.alembic_database_url:
            return self.alembic_database_url
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def effective_amqp_url(self) -> str:
        if self.amqp_url:
            return self.amqp_url
        return (
            f"amqp://{self.rabbitmq_default_user}:{self.rabbitmq_default_pass}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}/"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def effective_redis_url(self) -> str:
        if self.redis_url:
            return self.redis_url
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
