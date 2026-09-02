from __future__ import annotations

import asyncio
import logging

from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.aio import AIOConsumer, AIOProducer

from app.config import Settings
from app.kafka.common import (
    JsonMessageSerializer,
    LoggingMessageHandler,
    format_partitions,
    validate_message_count,
)
from app.kafka.interfaces import MessageHandler, MessageSerializer

logger = logging.getLogger(__name__)


class AsyncKafkaProducer:
    def __init__(
        self,
        settings: Settings,
        topic: str,
        serializer: MessageSerializer,
    ) -> None:
        self._settings = settings
        self._topic = topic
        self._serializer = serializer

    async def produce_messages(self, count: int) -> None:
        validate_message_count(count)
        producer = AIOProducer(
            {"bootstrap.servers": self._settings.kafka_bootstrap_servers},
            batch_size=min(count, 100),
            buffer_timeout=self._settings.kafka_message_delay_seconds,
        )
        delivery_futures: list[asyncio.Future[Message]] = []
        logger.info(
            "Async producer started: brokers=%s topic=%s count=%s",
            self._settings.kafka_bootstrap_servers,
            self._topic,
            count,
        )

        try:
            for number in range(1, count + 1):
                key, value = self._serializer.serialize(
                    number, f"Async study message #{number}"
                )
                delivery_future = await producer.produce(
                    topic=self._topic,
                    key=key,
                    value=value,
                )
                delivery_futures.append(delivery_future)
                logger.info(
                    "Queued async message key=%s value=%s",
                    key.decode(),
                    value.decode(),
                )
                await asyncio.sleep(self._settings.kafka_message_delay_seconds)

            await producer.flush()
            delivered_messages = await asyncio.gather(*delivery_futures)
            for message in delivered_messages:
                logger.info(
                    "Delivered to topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
            logger.info("Async producer finished; all %s messages delivered", count)
        finally:
            await producer.close()


class AsyncKafkaConsumer:
    def __init__(
        self,
        settings: Settings,
        topics: list[str],
        group_id: str,
        message_handler: MessageHandler,
    ) -> None:
        self._settings = settings
        self._topics = topics
        self._group_id = group_id
        self._message_handler = message_handler

    async def start_consume(self) -> None:
        consumer = AIOConsumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._group_id,
                "auto.offset.reset": self._settings.kafka_auto_offset_reset,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        logger.info(
            "Async consumer started: brokers=%s topics=%s group=%s (Ctrl+C to stop)",
            self._settings.kafka_bootstrap_servers,
            ", ".join(self._topics),
            self._group_id,
        )

        try:
            await consumer.subscribe(
                self._topics,
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            while True:
                message = await consumer.poll(self._settings.kafka_poll_timeout_seconds)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "End of partition topic=%s partition=%s offset=%s",
                            message.topic(),
                            message.partition(),
                            message.offset(),
                        )
                        continue
                    raise KafkaException(error)

                self._message_handler(message)
                await consumer.store_offsets(message=message)
                await consumer.commit(asynchronous=False)
        finally:
            await consumer.close()
            logger.info("Async consumer stopped")

    @staticmethod
    async def _on_assign(
        _consumer: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        logger.info("Partitions assigned: %s", format_partitions(partitions))

    @staticmethod
    async def _on_revoke(
        _consumer: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        logger.info("Partitions revoked: %s", format_partitions(partitions))


class AsyncKafkaClient:
    """Factory for configured asynchronous Kafka producers and consumers."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def create_producer(
        self,
        topic: str | None = None,
        serializer: MessageSerializer | None = None,
    ) -> AsyncKafkaProducer:
        return AsyncKafkaProducer(
            settings=self._settings,
            topic=topic or self._settings.kafka_async_topic,
            serializer=serializer or JsonMessageSerializer(),
        )

    def create_consumer(
        self,
        topics: list[str] | None = None,
        group_id: str | None = None,
        message_handler: MessageHandler | None = None,
    ) -> AsyncKafkaConsumer:
        return AsyncKafkaConsumer(
            settings=self._settings,
            topics=topics or [self._settings.kafka_async_topic],
            group_id=group_id or self._settings.kafka_async_consumer_group,
            message_handler=message_handler or LoggingMessageHandler(logger),
        )
