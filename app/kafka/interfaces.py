from __future__ import annotations

from typing import Protocol

from confluent_kafka import Message


class MessageSerializer(Protocol):
    """Converts an application message into Kafka key and value bytes."""

    def serialize(self, number: int, text: str) -> tuple[bytes, bytes]: ...


class MessageHandler(Protocol):
    """Processes one successfully consumed Kafka message."""

    def __call__(self, message: Message) -> None: ...


class SyncProducer(Protocol):
    def produce_messages(self, count: int) -> None: ...


class SyncConsumer(Protocol):
    def start_consume(self) -> None: ...


class AsyncProducer(Protocol):
    async def produce_messages(self, count: int) -> None: ...


class AsyncConsumer(Protocol):
    async def start_consume(self) -> None: ...
