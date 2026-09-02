from __future__ import annotations

import argparse
import logging

from app.config import configure_log, settings
from app.kafka import SyncKafkaClient

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send study messages to Kafka")
    parser.add_argument("count", type=int, help="number of messages to send")
    return parser.parse_args()


def main() -> None:
    configure_log()
    args = parse_args()
    producer = SyncKafkaClient(settings).create_producer()
    try:
        producer.produce_messages(args.count)
    except (ValueError, RuntimeError) as error:
        logger.error("Producer stopped: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
