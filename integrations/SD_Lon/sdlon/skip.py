import logging
from typing import Any
from typing import OrderedDict

from .config import get_changed_at_settings

logger = logging.getLogger("sdChangedAt")


def cpr_env_filter(entity: OrderedDict[str, Any]) -> bool:
    settings = get_changed_at_settings()

    cpr = entity["PersonCivilRegistrationIdentifier"]

    if settings.sd_exclude_cprs_mode:
        # The CPRs in the sd_cprs should be excluded from further processing
        # and all other CPRs should be processed
        process_cpr = cpr not in settings.sd_cprs
    else:
        # In this case "exclude mode" is False, i.e. "include mode" is True
        # which means that the cprs in sd_cprs are the ONLY ones that should
        # be processed further
        process_cpr = cpr in settings.sd_cprs

    if not process_cpr and settings.sd_exclude_cprs_mode:
        logger.warning(f"*** SKIPPING employee with cpr={cpr} ***")

    return process_cpr
