from __future__ import annotations

import argparse
import asyncio
import logging

from confluent_kafka import KafkaException

from app.config import configure_log, settings
from app.kafka import AsyncKafkaClient

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send study messages to Kafka asynchronously"
    )
    parser.add_argument("count", type=int, help="number of messages to send")
    return parser.parse_args()


async def run(count: int) -> None:
    producer = AsyncKafkaClient(settings).create_producer()
    await producer.produce_messages(count)


def main() -> None:
    configure_log()
    args = parse_args()
    try:
        asyncio.run(run(args.count))
    except (ValueError, KafkaException) as error:
        logger.error("Async producer stopped: %s", error)
        raise SystemExit(1) from error
    except KeyboardInterrupt:
        logger.info("Async producer interrupted")


if __name__ == "__main__":
    main()
