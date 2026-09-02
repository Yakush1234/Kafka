from __future__ import annotations

import logging

from confluent_kafka import KafkaException

from app.config import configure_log, settings
from app.kafka import SyncKafkaClient

logger = logging.getLogger(__name__)


def main() -> None:
    configure_log()
    consumer = SyncKafkaClient(settings).create_consumer()
    try:
        consumer.start_consume()
    except KafkaException as error:
        logger.exception("Consumer failed: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
