from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import OrderedDict

from integrations.SD_Lon.sd_common import ensure_list
from integrations.SD_Lon.sd_common import read_employment_at


def engagement_components(engagement_info) -> Tuple[str, Dict[str, List[Any]]]:
    employment_id = engagement_info["EmploymentIdentifier"]

    return employment_id, {
        "status_list": ensure_list(engagement_info.get("EmploymentStatus", [])),
        "professions": ensure_list(engagement_info.get("Profession", [])),
        "departments": ensure_list(engagement_info.get("EmploymentDepartment", [])),
        "working_time": ensure_list(engagement_info.get("WorkingTime", [])),
    }


def get_employment_from_date(
    employment: OrderedDict,
    employment_date_as_engagement_start_date: bool
) -> datetime:

    # Make sure we do not have multiple EmploymentStatuses
    assert isinstance(employment["EmploymentStatus"], OrderedDict)
    
    date = employment["EmploymentStatus"]["ActivationDate"]
    if employment_date_as_engagement_start_date:
        date = employment["EmploymentDate"]
    return datetime.strptime(date, "%Y-%m-%d")


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
