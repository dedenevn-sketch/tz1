from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "dev-api-key"

    outbox_relay_enabled: bool = True
    outbox_poll_interval_seconds: float = 1.0

    process_delay_min_seconds: float = 2.0
    process_delay_max_seconds: float = 5.0
    process_success_rate: float = 0.9

    webhook_max_attempts: int = 3
    webhook_backoff_base_seconds: float = 1.0
    webhook_timeout_seconds: float = 10.0

    consumer_max_attempts: int = 3
    consumer_backoff_base_seconds: float = 1.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
