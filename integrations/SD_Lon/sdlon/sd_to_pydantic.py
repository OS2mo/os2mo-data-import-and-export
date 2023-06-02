from typing import OrderedDict, Any

from sdlon.models import EmploymentWithTelephoneNumberIdentifier, SDBasePerson
from sdlon.sd_common import ensure_list


def convert_to_sd_base_person(person: OrderedDict[str, Any]) -> SDBasePerson:
    """
    Convert the raw OrderedDict from SD to a Pydantic base model
    representing the SD person.

    Args:
        person: the person from SD Løn

    Returns:
        Basic SD person
    """

    telephone_number_identifiers = []
    employments = ensure_list(person.get("Employment", []))
    for employment in employments:
        contact_information = employment.get("ContactInformation", {})
        telephone_number_ids = ensure_list(
            contact_information.get("TelephoneNumberIdentifier", [])
        )
        telephone_number_identifiers.append(
            EmploymentWithTelephoneNumberIdentifier(
                employment_identifier=employment["EmploymentIdentifier"],
                telephone_number_ids=telephone_number_ids,
            )
        )

    return SDBasePerson(
        cpr=person["PersonCivilRegistrationIdentifier"],
        given_name=person.get("PersonGivenName"),
        surname=person.get("PersonSurnameName"),
        emp_with_telephone_number_identifiers=telephone_number_identifiers,
    )
