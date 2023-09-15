import logging

import structlog


def setup_logging(
        log_level_name: str,
) -> None:
    _log_level_value = logging.getLevelName(log_level_name)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(_log_level_value),
    )
