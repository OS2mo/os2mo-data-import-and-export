import logging
import re
from enum import Enum

import structlog


CPR_REGEX = re.compile("[0-9]{10}")


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def anonymize_cpr(cpr: str) -> str:
    assert CPR_REGEX.match(cpr)
    return cpr[:6] + "xxxx"


def setup_logging(log_level: LogLevel) -> None:
    log_level_value = logging.getLevelName(log_level.value)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level_value)
    )
