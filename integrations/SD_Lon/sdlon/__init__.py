import logging
import structlog

from .config import get_changed_at_settings

_log_level_name = get_changed_at_settings().sd_log_level
_log_level_value = logging.getLevelName(_log_level_name)

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(_log_level_value)
)
