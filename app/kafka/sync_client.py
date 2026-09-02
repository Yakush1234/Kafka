from __future__ import annotations

import logging
import time

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)

from app.config import Settings
from app.kafka.common import (
    JsonMessageSerializer,
    LoggingMessageHandler,
    format_partitions,
    validate_message_count,
)
from app.kafka.interfaces import MessageHandler, MessageSerializer

logger = logging.getLogger(__name__)


class SyncKafkaProducer:
    def __init__(
        self,
        settings: Settings,
        topic: str,
        serializer: MessageSerializer,
    ) -> None:
        self._settings = settings
        self._topic = topic
        self._serializer = serializer

    def produce_messages(self, count: int) -> None:
        validate_message_count(count)
        producer = Producer(
            {"bootstrap.servers": self._settings.kafka_bootstrap_servers}
        )
        logger.info(
            "Producer started: brokers=%s topic=%s count=%s",
            self._settings.kafka_bootstrap_servers,
            self._topic,
            count,
        )

        for number in range(1, count + 1):
            key, value = self._serializer.serialize(number, f"Study message #{number}")
            producer.poll(0)
            producer.produce(
                topic=self._topic,
                key=key,
                value=value,
                callback=self._delivery_report,
            )
            logger.info("Queued message key=%s value=%s", key.decode(), value.decode())
            time.sleep(self._settings.kafka_message_delay_seconds)

        remaining = producer.flush(timeout=10)
        if remaining:
            raise RuntimeError(
                f"Failed to deliver {remaining} message(s) before timeout"
            )
        logger.info("Producer finished; all %s messages delivered", count)

    @staticmethod
    def _delivery_report(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            logger.error("Delivery failed: %s", error)
            return
        logger.info(
            "Delivered to topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


class SyncKafkaConsumer:
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

    def start_consume(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._group_id,
                "auto.offset.reset": self._settings.kafka_auto_offset_reset,
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe(
            self._topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        logger.info(
            "Consumer started: brokers=%s topics=%s group=%s (Ctrl+C to stop)",
            self._settings.kafka_bootstrap_servers,
            ", ".join(self._topics),
            self._group_id,
        )

        try:
            while True:
                message = consumer.poll(self._settings.kafka_poll_timeout_seconds)
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
        except KeyboardInterrupt:
            logger.info("Stop requested")
        finally:
            consumer.close()
            logger.info("Consumer stopped")

    @staticmethod
    def _on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info("Partitions assigned: %s", format_partitions(partitions))

    @staticmethod
    def _on_revoke(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info("Partitions revoked: %s", format_partitions(partitions))


class SyncKafkaClient:
    """Factory for configured synchronous Kafka producers and consumers."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def create_producer(
        self,
        topic: str | None = None,
        serializer: MessageSerializer | None = None,
    ) -> SyncKafkaProducer:
        return SyncKafkaProducer(
            settings=self._settings,
            topic=topic or self._settings.kafka_topic,
            serializer=serializer or JsonMessageSerializer(),
        )

    def create_consumer(
        self,
        topics: list[str] | None = None,
        group_id: str | None = None,
        message_handler: MessageHandler | None = None,
    ) -> SyncKafkaConsumer:
        return SyncKafkaConsumer(
            settings=self._settings,
            topics=topics or [self._settings.kafka_topic],
            group_id=group_id or self._settings.kafka_consumer_group,
            message_handler=message_handler or LoggingMessageHandler(logger),
        )
