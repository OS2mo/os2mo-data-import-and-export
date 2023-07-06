import logging
from typing import Any
from typing import OrderedDict

from more_itertools import one

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
        logger.warning(f"*** SKIPPING employee with cpr={cpr[:6]} ***")

    return process_cpr


def skip_fictional_users(entity) -> bool:
    cpr = entity["PersonCivilRegistrationIdentifier"]
    if cpr[-4:] == "0000":
        logger.warning("Skipping fictional user: {}".format(cpr))
        return False
    return True


def skip_job_position_id(
    profession: OrderedDict[str, Any], job_pos_ids_to_skip: list[str]
) -> bool:
    """
    Check if SD JobPositionIdentifier is in the list to skip,
    i.e. the list provided via the environment variable
    SD_SKIP_EMPLOYMENT_TYPES

    Args:
        profession: a "Profession" in the list of professions in the
          SD employment.
        job_pos_ids_to_skip: list of SD JobPositionIdentifiers to skip

    Returns:
        True if the SD profession should be skipped and false otherwise.
    """

    job_pos_id = profession.get("JobPositionIdentifier")
    if job_pos_id in job_pos_ids_to_skip:
        return True

    return False
