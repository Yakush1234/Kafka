from __future__ import annotations

import asyncio
import logging

from confluent_kafka import KafkaException

from app.config import configure_log, settings
from app.kafka import AsyncKafkaClient

logger = logging.getLogger(__name__)


async def run() -> None:
    consumer = AsyncKafkaClient(settings).create_consumer()
    await consumer.start_consume()


def main() -> None:
    configure_log()
    try:
        asyncio.run(run())
    except KafkaException as error:
        logger.exception("Async consumer failed: %s", error)
        raise SystemExit(1) from error
    except KeyboardInterrupt:
        logger.info("Stop requested")


if __name__ == "__main__":
    main()
