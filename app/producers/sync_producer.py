from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer

from app.config import configure_log, settings

logger = logging.getLogger(__name__)


def delivery_report(error: KafkaError | None, message: Message) -> None:
    """Called by poll()/flush() when Kafka acknowledges a message."""
    if error is not None:
        logger.error("Delivery failed: %s", error)
        return

    logger.info(
        "Delivered to topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def produce_messages(count: int) -> None:
    if count < 1:
        raise ValueError("Message count must be greater than zero")

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    logger.info(
        "Producer started: brokers=%s topic=%s count=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        count,
    )

    for number in range(1, count + 1):
        payload = {
            "message_number": number,
            "text": f"Study message #{number}",
            "created_at": datetime.now(UTC).isoformat(),
        }
        value = json.dumps(payload, ensure_ascii=False).encode()

        # poll(0) serves delivery callbacks without blocking the producer.
        producer.poll(0)
        producer.produce(
            topic=settings.kafka_topic,
            key=str(number).encode(),
            value=value,
            callback=delivery_report,
        )
        logger.info("Queued message key=%s value=%s", number, payload)
        time.sleep(settings.kafka_message_delay_seconds)

    remaining = producer.flush(timeout=10)
    if remaining:
        raise RuntimeError(f"Failed to deliver {remaining} message(s) before timeout")
    logger.info("Producer finished; all %s messages delivered", count)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send study messages to Kafka")
    parser.add_argument("count", type=int, help="number of messages to send")
    return parser.parse_args()


def main() -> None:
    configure_log()
    args = parse_args()
    try:
        produce_messages(args.count)
    except (ValueError, RuntimeError) as error:
        logger.error("Producer stopped: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
