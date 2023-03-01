import datetime
import hashlib
import logging
import uuid
from enum import Enum
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import OrderedDict
from typing import Union

import requests
import xmltodict
from ra_utils.load_settings import load_settings

from .config import CommonSettings

logger = logging.getLogger("sdCommon")


@lru_cache(maxsize=None)
def sd_lookup_settings():
    settings = load_settings()

    institution_identifier = settings["integrations.SD_Lon.institution_identifier"]
    if not institution_identifier:
        raise ValueError("Missing setting, institution_identifier")

    sd_user = settings["integrations.SD_Lon.sd_user"]
    if not sd_user:
        raise ValueError("Missing setting, sd_user")

    sd_password = settings["integrations.SD_Lon.sd_password"]
    if not sd_password:
        raise ValueError("Missing setting, sd_password")

    return institution_identifier, sd_user, sd_password


def sd_lookup(
    url: str,
    settings: Optional[CommonSettings] = None,
    params: Optional[Dict[str, Any]] = None,
) -> OrderedDict:
    """Fire a requests against SD."""
    logger.info("Retrieve: {}".format(url))
    logger.debug("Params: {}".format(params))

    params = params or dict()

    BASE_URL = "https://service.sd.dk/sdws/"
    full_url = BASE_URL + url

    # Use settings if provided as an argument to this function
    # Currently, we only have Pydantic settings models for sd_importer.py
    # and sd_changed_at.py, so this logic is required for now, since the
    # sd_lookup function is used elsewhere too
    if settings is not None:
        institution_identifier = settings.sd_institution_identifier
        sd_user = settings.sd_user
        sd_password = settings.sd_password.get_secret_value()
    else:
        institution_identifier, sd_user, sd_password = sd_lookup_settings()

    payload = {
        "InstitutionIdentifier": institution_identifier,
    }
    payload.update(params)
    auth = (sd_user, sd_password)
    response = requests.get(
        full_url,
        params=payload,
        auth=auth,
    )
    logger.debug("Response: {}".format(response.text))

    dict_response = xmltodict.parse(response.text)

    if url in dict_response:
        xml_response = dict_response[url]
    else:
        msg = "SD api error, envelope: {}, response: {}"
        logger.error(msg.format(dict_response["Envelope"], response.text))
        raise Exception(msg.format(dict_response["Envelope"], response.text))
    logger.debug("Done with {}".format(url))
    return xml_response


def calc_employment_id(employment):
    employment_id = employment["EmploymentIdentifier"]
    try:
        employment_number = int(employment_id)
    except ValueError:  # Job id is not a number?
        employment_number = 999999

    employment_id = {"id": employment_id, "value": employment_number}
    return employment_id


def mora_assert(response):
    """Check response is as expected."""
    assert response.status_code in (200, 201, 400, 404), response.status_code
    if response.status_code == 400:
        # Check actual response
        assert (
            response.text.find("not give raise to a new registration") > 0
        ), response.text
        logger.debug("Request had no effect")
    return None


def generate_uuid(value, org_id_prefix, org_name=None):
    """
    Code almost identical to this also lives in the Opus importer.
    """
    # TODO: Refactor to avoid duplication
    if org_id_prefix:
        base_hash = hashlib.md5(org_id_prefix.encode())
    else:
        base_hash = hashlib.md5(org_name.encode())

    base_digest = base_hash.hexdigest()
    base_uuid = uuid.UUID(base_digest)

    combined_value = (str(base_uuid) + str(value)).encode()
    value_hash = hashlib.md5(combined_value)
    value_digest = value_hash.hexdigest()
    value_uuid = str(uuid.UUID(value_digest))
    return value_uuid


def primary_types(helper):
    """
    Read the engagement types from MO and match them up against the four
    known types in the SD->MO import.

    Args:
        helper: An instance of mora-helpers.

    Returns:
        A dict matching up the engagement types with LoRa class uuids (
        i.e. UUIDs of facets).

    Example:
        An example return value:
        ```python
        {
            "primary": "697c8838-ba0f-4e74-90f8-4e7c31d4e7e7",
            "non_primary": "b9543f90-9511-494b-bbf5-f15678502c2d",
            "no_salary": "88589e84-5736-4f8c-9c0c-2e29046d7471",
            "fixed_primary": "c95a1999-9f95-4458-a218-e9c96e7ad3db",
        }
        ```
    """

    # These constants are global in all SD municipalities (because they are created
    # by the SD->MO importer.
    PRIMARY = "Ansat"
    NO_SALARY = "status0"
    NON_PRIMARY = "non-primary"
    FIXED_PRIMARY = "explicitly-primary"

    logger.info("Read primary types")
    primary = None
    no_salary = None
    non_primary = None
    fixed_primary = None

    primary_types = helper.read_classes_in_facet("primary_type")
    for primary_type in primary_types[0]:
        if primary_type["user_key"] == PRIMARY:
            primary = primary_type["uuid"]
        if primary_type["user_key"] == NON_PRIMARY:
            non_primary = primary_type["uuid"]
        if primary_type["user_key"] == NO_SALARY:
            no_salary = primary_type["uuid"]
        if primary_type["user_key"] == FIXED_PRIMARY:
            fixed_primary = primary_type["uuid"]

    type_uuids = {
        "primary": primary,
        "non_primary": non_primary,
        "no_salary": no_salary,
        "fixed_primary": fixed_primary,
    }
    if None in type_uuids.values():
        raise Exception("Missing primary types: {}".format(type_uuids))
    return type_uuids


class EmploymentStatus(Enum):
    """Corresponds to EmploymentStatusCode from SD.

    Employees usually start in AnsatUdenLoen, and then change to AnsatMedLoen.
    This will usually happen once they actually have their first day at work.

    From AnsatMedLoen they can somewhat freely transfer to the other statusses.
    This includes transfering back to AnsatMedLoen from any other status.

    Note for instance, that it is entirely possible to be Ophoert and then get
    hired back, and thus go from Ophoert to AnsatMedLoen.

    There is only one terminal state, namely Slettet, wherefrom noone will
    return. This state is invoked from status 7-8-9 after a few years.

    Status Doed will probably only migrate to status slettet, but there are no
    guarantees given.
    """

    # This status most likely represent not yet being at work
    AnsatUdenLoen = "0"

    # These statusses represent being at work
    AnsatMedLoen = "1"
    Orlov = "3"

    # These statusses represent being let go
    Migreret = "7"
    Ophoert = "8"
    Doed = "9"

    # This status is the special terminal state
    Slettet = "S"

    @staticmethod
    def employeed() -> List["EmploymentStatus"]:
        return [
            EmploymentStatus.AnsatUdenLoen,
            EmploymentStatus.AnsatMedLoen,
            EmploymentStatus.Orlov,
        ]

    @staticmethod
    def let_go() -> List["EmploymentStatus"]:
        return [
            EmploymentStatus.Migreret,
            EmploymentStatus.Ophoert,
            EmploymentStatus.Doed,
        ]

    @staticmethod
    def on_payroll() -> List["EmploymentStatus"]:
        return [EmploymentStatus.AnsatMedLoen, EmploymentStatus.Orlov]


def skip_fictional_users(entity):
    cpr = entity["PersonCivilRegistrationIdentifier"]
    if cpr[-4:] == "0000":
        logger.warning("Skipping fictional user: {}".format(cpr))
        return False
    return True


def ensure_list(element):
    if not isinstance(element, list):
        return [element]
    return element


# We will get to the Pydantic models later...
def read_employment_at(
    effective_date: datetime.date,
    settings: CommonSettings,
    employment_id: Optional[str] = None,
    status_active_indicator: bool = True,
    status_passive_indicator: bool = True,
) -> Union[OrderedDict, List[OrderedDict], None]:
    url = "GetEmployment20111201"
    params = {
        "EffectiveDate": effective_date.strftime("%d.%m.%Y"),
        "StatusActiveIndicator": str(status_active_indicator).lower(),
        "StatusPassiveIndicator": str(status_passive_indicator).lower(),
        "DepartmentIndicator": "true",
        "EmploymentStatusIndicator": "true",
        "ProfessionIndicator": "true",
        "WorkingTimeIndicator": "true",
        "UUIDIndicator": "true",
        "SalaryAgreementIndicator": "false",
        "SalaryCodeGroupIndicator": "false",
    }

    if employment_id:
        params.update({"EmploymentIdentifier": employment_id})

    response = sd_lookup(url, settings=settings, params=params)
    return response.get("Person")
