from __future__ import annotations

import logging

from app.config.config import settings

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_log(level: str | int | None = None) -> None:
    """Configure application logging with an optional minimum level override."""
    logging.basicConfig(
        level=settings.log_level.upper() if level is None else level,
        format=LOG_FORMAT,
    )
