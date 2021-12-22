from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from integrations.SD_Lon.sd_common import ensure_list


def engagement_components(engagement_info) -> Tuple[str, Dict[str, List[Any]]]:
    employment_id = engagement_info["EmploymentIdentifier"]

    return employment_id, {
        "status_list": ensure_list(engagement_info.get("EmploymentStatus", [])),
        "professions": ensure_list(engagement_info.get("Profession", [])),
        "departments": ensure_list(engagement_info.get("EmploymentDepartment", [])),
        "working_time": ensure_list(engagement_info.get("WorkingTime", [])),
    }


def update_existing_engagement(sd_updater, mo_engagement, sd_engagement, person_uuid) -> None:
    sd_updater.edit_engagement_department(sd_engagement, mo_engagement, person_uuid)
    sd_updater.edit_engagement_profession(sd_engagement, mo_engagement)
    sd_updater.edit_engagement_type(sd_engagement, mo_engagement)
    sd_updater.edit_engagement_worktime(sd_engagement, mo_engagement)


def create_engagement(sd_updater, employment_id, person_uuid) -> None:
    # Call SD to get SD employment
    sd_employment_payload = sd_updater.read_employment_at(employment_id, datetime.now())
    cpr = sd_employment_payload["PersonCivilRegistrationIdentifier"]
    sd_employment = sd_employment_payload.get("Employment")
    status = sd_employment.get("EmploymentStatus")

    # Is it possible that sd_employment or status is None?...
    assert sd_employment
    assert status

    # Not sure what to do if several statuses are returned...
    assert not isinstance(status, list)

    # Call MO to create corresponding engagement in MO
    sd_updater.create_new_engagement(sd_employment, status, cpr, person_uuid)
