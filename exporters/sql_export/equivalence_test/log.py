import logging.config
from enum import Enum
from uuid import uuid4

import structlog

# Unique log context for this application
APP_LOG_CONTEXT = uuid4()


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def log_filter(logger, method_name, event_dict):
    # Only log from our application log context
    log_context = event_dict.pop("log_context", None)
    if log_context == APP_LOG_CONTEXT:
        return event_dict
    raise structlog.DropEvent


def get_logger():
    """
    Use this function to get a logger instead of using structlog.get_logger() directly
    """
    return structlog.get_logger().bind(log_context=APP_LOG_CONTEXT)


def setup_logging(log_level: str) -> None:
    log_level_value = logging.getLevelName(log_level)

    # Disable logging from imported modules that use Pythons stdlib logging
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,  # This line does not really seem to work
            "root": {"level": logging.CRITICAL + 1},
        }
    )

    structlog.configure(
        processors=[
            log_filter,
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level_value),
    )
