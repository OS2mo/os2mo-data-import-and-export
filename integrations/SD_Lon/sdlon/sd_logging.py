import logging

import structlog

from sdlon.models import LogLevel


def get_logger(name: str, log_level: LogLevel = LogLevel.debug):
    log_level_value = logging.getLevelName(log_level.value)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level_value)
    )

    return structlog.get_logger(name)
