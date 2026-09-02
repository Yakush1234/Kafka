from __future__ import annotations

import asyncio
import logging

from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.aio import AIOConsumer

from app.config import configure_log, settings

logger = logging.getLogger(__name__)


def format_partitions(partitions: list[TopicPartition]) -> str:
    """Avoid TopicPartition.__repr__ formatting issues on Windows."""
    return ", ".join(
        f"{partition.topic}[{partition.partition}]@{partition.offset}"
        for partition in partitions
    )


async def on_assign(_consumer: AIOConsumer, partitions: list[TopicPartition]) -> None:
    logger.info("Partitions assigned: %s", format_partitions(partitions))


async def on_revoke(_consumer: AIOConsumer, partitions: list[TopicPartition]) -> None:
    logger.info("Partitions revoked: %s", format_partitions(partitions))


def log_message(message: Message) -> None:
    raw_key = message.key()
    raw_value = message.value()
    key = raw_key.decode() if raw_key is not None else None
    value = raw_value.decode() if raw_value is not None else None
    logger.info(
        "Received topic=%s partition=%s offset=%s key=%s value=%s",
        message.topic(),
        message.partition(),
        message.offset(),
        key,
        value,
    )


async def consume_messages() -> None:
    consumer = AIOConsumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_async_consumer_group,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "partition.assignment.strategy": "cooperative-sticky",
        }
    )
    logger.info(
        "Async consumer started: brokers=%s topic=%s group=%s (Ctrl+C to stop)",
        settings.kafka_bootstrap_servers,
        settings.kafka_async_topic,
        settings.kafka_async_consumer_group,
    )

    try:
        await consumer.subscribe(
            [settings.kafka_async_topic],
            on_assign=on_assign,
            on_revoke=on_revoke,
        )
        while True:
            message = await consumer.poll(settings.kafka_poll_timeout_seconds)
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

            # Store and commit only after successful processing: at-least-once.
            log_message(message)
            await consumer.store_offsets(message=message)
            await consumer.commit(asynchronous=False)
    finally:
        await consumer.close()
        logger.info("Async consumer stopped")


def main() -> None:
    configure_log()
    try:
        asyncio.run(consume_messages())
    except KafkaException as error:
        logger.exception("Async consumer failed: %s", error)
        raise SystemExit(1) from error
    except KeyboardInterrupt:
        logger.info("Stop requested")


if __name__ == "__main__":
    main()
