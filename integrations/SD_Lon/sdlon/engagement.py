import re
from datetime import datetime
from operator import itemgetter
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import OrderedDict
from typing import Tuple

from .sd_common import ensure_list
from .sd_common import read_employment_at

INTERNAL_EMPLOYEE_REGEX = re.compile("[0-9]+")


def engagement_components(engagement_info) -> Tuple[str, Dict[str, List[Any]]]:
    employment_id = engagement_info["EmploymentIdentifier"]

    return employment_id, {
        "status_list": ensure_list(engagement_info.get("EmploymentStatus", [])),
        "professions": ensure_list(engagement_info.get("Profession", [])),
        "departments": ensure_list(engagement_info.get("EmploymentDepartment", [])),
        "working_time": ensure_list(engagement_info.get("WorkingTime", [])),
    }


def update_existing_engagement(
    sd_updater, mo_engagement, sd_engagement, person_uuid
) -> None:
    sd_updater.edit_engagement_department(sd_engagement, mo_engagement, person_uuid)
    sd_updater.edit_engagement_profession(sd_engagement, mo_engagement)
    sd_updater.edit_engagement_type(sd_engagement, mo_engagement)
    sd_updater.edit_engagement_worktime(sd_engagement, mo_engagement)


def create_engagement(sd_updater, employment_id, person_uuid) -> None:
    # Call SD to get SD employment
    sd_employment_payload = read_employment_at(
        datetime.now().date(), settings=sd_updater.settings, employment_id=employment_id
    )
    if sd_employment_payload is None:
        return

    assert not isinstance(sd_employment_payload, list)

    cpr = sd_employment_payload["PersonCivilRegistrationIdentifier"]
    sd_employment = sd_employment_payload.get("Employment")
    status = sd_employment.get("EmploymentStatus")  # type: ignore

    # Is it possible that sd_employment or status is None?...
    assert sd_employment
    assert status

    # Not sure what to do if several statuses are returned...
    assert not isinstance(status, list)

    # Call MO to create corresponding engagement in MO
    sd_updater.create_new_engagement(sd_employment, status, cpr, person_uuid)


def _is_external(employment_id: str) -> bool:
    """
    Check if the SD employee is an external employee. This is the
    case (at least in some municipalities...) if the EmploymentIdentifier
    contains letters.

    Args:
         employment_id: the SD EmploymentIdentifier

    Returns:
        True of the employment_id contains letters and False otherwise
    """

    match = INTERNAL_EMPLOYEE_REGEX.match(employment_id)
    return match is None


def is_employment_id_and_no_salary_minimum_consistent(
    engagement: OrderedDict, no_salary_minimum: Optional[int] = None
) -> bool:
    """
    Check that the external SD employees have JobPositionIdentifiers
    consistent with no_salary_limit
    (see https://os2web.atlassian.net/browse/MO-245).

    Args:
        engagement: the SD employment
        no_salary_minimum: the minimum allowed JobPositionIdentifier
          for external SD employees.

    Returns:
        True if the provided values are consistent and False otherwise.
    """

    if no_salary_minimum is None:
        return True

    employment_id, eng_components = engagement_components(engagement)
    professions = eng_components["professions"]
    if not professions:
        return True

    job_pos_ids_strings = map(itemgetter("JobPositionIdentifier"), professions)
    job_pos_ids = map(int, job_pos_ids_strings)

    def is_consistent(job_pos_id: int) -> bool:
        if _is_external(employment_id):
            return job_pos_id >= no_salary_minimum  # type: ignore
        return job_pos_id < no_salary_minimum  # type: ignore

    consistent = map(is_consistent, job_pos_ids)

    return all(consistent)
