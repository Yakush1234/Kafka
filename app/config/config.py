from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "study.messages"
    kafka_consumer_group: str = "study-consumers"
    kafka_auto_offset_reset: str = "earliest"
    kafka_message_delay_seconds: float = Field(default=0.1, ge=0)
    kafka_poll_timeout_seconds: float = Field(default=1.0, gt=0)
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
