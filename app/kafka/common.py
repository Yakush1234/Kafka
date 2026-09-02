from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from confluent_kafka import Message, TopicPartition


class JsonMessageSerializer:
    """Serializes study messages as UTF-8 encoded JSON."""

    def serialize(self, number: int, text: str) -> tuple[bytes, bytes]:
        payload = {
            "message_number": number,
            "text": text,
            "created_at": datetime.now(UTC).isoformat(),
        }
        return str(number).encode(), json.dumps(payload, ensure_ascii=False).encode()


class LoggingMessageHandler:
    """Logs consumed messages; replace it to add real business processing."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def __call__(self, message: Message) -> None:
        raw_key = message.key()
        raw_value = message.value()
        key = raw_key.decode() if raw_key is not None else None
        value = raw_value.decode() if raw_value is not None else None
        self._logger.info(
            "Received topic=%s partition=%s offset=%s key=%s value=%s",
            message.topic(),
            message.partition(),
            message.offset(),
            key,
            value,
        )


def format_partitions(partitions: list[TopicPartition]) -> str:
    """Format partitions without their broken Windows __repr__ implementation."""
    return ", ".join(
        f"{partition.topic}[{partition.partition}]@{partition.offset}"
        for partition in partitions
    )


def validate_message_count(count: int) -> None:
    if count < 1:
        raise ValueError("Message count must be greater than zero")
