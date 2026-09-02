from __future__ import annotations

import logging

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from app.config import settings

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def format_partitions(partitions: list[TopicPartition]) -> str:
    """Avoid TopicPartition.__repr__ formatting issues on Windows."""
    return ", ".join(
        f"{partition.topic}[{partition.partition}]@{partition.offset}"
        for partition in partitions
    )


def log_assignment(
    _consumer: Consumer, partitions: list[TopicPartition]
) -> None:
    logger.info("Partitions assigned: %s", format_partitions(partitions))


def log_revocation(
    _consumer: Consumer, partitions: list[TopicPartition]
) -> None:
    logger.info("Partitions revoked: %s", format_partitions(partitions))


def consume_messages() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(
        [settings.kafka_topic],
        on_assign=log_assignment,
        on_revoke=log_revocation,
    )
    logger.info(
        "Consumer started: brokers=%s topic=%s group=%s (Ctrl+C to stop)",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.kafka_consumer_group,
    )

    try:
        while True:
            message = consumer.poll(settings.kafka_poll_timeout_seconds)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "End of partition topic=%s partition=%s offset=%s",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )
                    continue
                raise KafkaException(message.error())

            log_message(message)
    except KeyboardInterrupt:
        logger.info("Stop requested")
    finally:
        # close() leaves the consumer group cleanly and commits current offsets.
        consumer.close()
        logger.info("Consumer stopped")


def log_message(message: Message) -> None:
    key = message.key().decode() if message.key() is not None else None
    value = message.value().decode() if message.value() is not None else None
    logger.info(
        "Received topic=%s partition=%s offset=%s key=%s value=%s",
        message.topic(),
        message.partition(),
        message.offset(),
        key,
        value,
    )


def main() -> None:
    configure_logging()
    try:
        consume_messages()
    except KafkaException as error:
        logger.exception("Consumer failed: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
