import structlog

from sdlon.config import get_common_settings


def configure_logging():
    settings = get_common_settings()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            settings.sd_log_level.value
        )
    )


def get_logger(name: str):
    return structlog.get_logger(name)
