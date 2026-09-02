from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaException, Message
from confluent_kafka.aio import AIOProducer

from app.config import settings

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


async def produce_messages(count: int) -> None:
    if count < 1:
        raise ValueError("Message count must be greater than zero")

    producer = AIOProducer(
        {"bootstrap.servers": settings.kafka_bootstrap_servers},
        batch_size=min(count, 100),
        buffer_timeout=settings.kafka_message_delay_seconds,
    )
    delivery_futures: list[asyncio.Future[Message]] = []
    logger.info(
        "Async producer started: brokers=%s topic=%s count=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_async_topic,
        count,
    )

    try:
        for number in range(1, count + 1):
            payload = {
                "message_number": number,
                "text": f"Async study message #{number}",
                "created_at": datetime.now(UTC).isoformat(),
            }
            value = json.dumps(payload, ensure_ascii=False).encode()
            delivery_future = await producer.produce(
                topic=settings.kafka_async_topic,
                key=str(number).encode(),
                value=value,
            )
            delivery_futures.append(delivery_future)
            logger.info("Queued async message key=%s value=%s", number, payload)
            await asyncio.sleep(settings.kafka_message_delay_seconds)

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send study messages to Kafka asynchronously"
    )
    parser.add_argument("count", type=int, help="number of messages to send")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    try:
        asyncio.run(produce_messages(args.count))
    except (ValueError, KafkaException) as error:
        logger.error("Async producer stopped: %s", error)
        raise SystemExit(1) from error
    except KeyboardInterrupt:
        logger.info("Async producer interrupted")


if __name__ == "__main__":
    main()
