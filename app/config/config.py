from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "kafka-study"
    app_env: str = "development"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "study-topic"
    kafka_consumer_group: str = "study-group"
    kafka_client_id: str = "study-client"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


settings = Settings()
