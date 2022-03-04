from datetime import datetime
from typing import Any, Union, Optional
from typing import Dict
from typing import List
from typing import Tuple
import re

from integrations.SD_Lon.sd_common import ensure_list
from integrations.SD_Lon.sd_common import read_employment_at

from typing import OrderedDict

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
        datetime.now().date(), employment_id=employment_id
    )

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


def is_external(employment_id: str) -> bool:
    """
    Check if the SD employee is an external employee. This is the
    case (at least in some municipalities...) if the EmploymentIdentifier
    contains letters.

    Args:
         employment_id: the SD EmploymentIdentifier

    Returns:
        True of the employment_id contains letters and False otherwise
    """

    match = re.compile("[0-9]+").match(employment_id)
    return True if match is None else False


def is_employment_id_and_no_salary_minimum_consistent(
    engagement: OrderedDict, no_salary_minimum: Union[int, None]
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

    employment_id = engagement["EmploymentIdentifier"]
    profession = engagement.get("Profession")
    assert profession, "Profession not found in Employment"
    job_pos_id_str = profession.get("JobPositionIdentifier")
    assert job_pos_id_str, "JobPositionIdentifier not found in Profession"

    job_pos_id = int(job_pos_id_str)

    if no_salary_minimum is None:
        return True

    if is_external(employment_id):
        return job_pos_id >= no_salary_minimum
    return job_pos_id < no_salary_minimum
