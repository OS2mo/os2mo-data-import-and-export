from sdlon.models import SDBasePerson, EmploymentWithTelephoneNumberIdentifier
from sdlon.sd_to_pydantic import convert_to_sd_base_person


def test_convert_to_sd_base_person__no_contact_info():
    # Arrange
    person = {
        "PersonCivilRegistrationIdentifier": "1111111111",
        "PersonGivenName": "Bruce",
        "PersonSurnameName": "Lee",
        "Employment": {
            "EmploymentIdentifier": "12345",
        },
    }

    # Act
    sd_base_person = convert_to_sd_base_person(person)

    # Assert
    assert sd_base_person == SDBasePerson(
        cpr="1111111111",
        given_name="Bruce",
        surname="Lee",
        emp_with_telephone_number_identifiers=[
            EmploymentWithTelephoneNumberIdentifier(
                employment_identifier="12345", telephone_number_ids=[]
            )
        ],
    )


def test_convert_to_sd_base_person__with_contact_info():
    # Arrange
    person = {
        "PersonCivilRegistrationIdentifier": "1111111111",
        "PersonGivenName": "Bruce",
        "PersonSurnameName": "Lee",
        "Employment": {
            "EmploymentIdentifier": "12345",
            "ContactInformation": {"TelephoneNumberIdentifier": ["12345678", "14"]},
        },
    }

    # Act
    sd_base_person = convert_to_sd_base_person(person)

    # Assert
    assert sd_base_person == SDBasePerson(
        cpr="1111111111",
        given_name="Bruce",
        surname="Lee",
        emp_with_telephone_number_identifiers=[
            EmploymentWithTelephoneNumberIdentifier(
                employment_identifier="12345", telephone_number_ids=["12345678", "14"]
            )
        ],
    )


def test_convert_to_sd_base_person__with_contact_info_multiple_employments():
    # Arrange
    person = {
        "PersonCivilRegistrationIdentifier": "1111111111",
        "PersonGivenName": "Bruce",
        "PersonSurnameName": "Lee",
        "Employment": [
            {
                "EmploymentIdentifier": "12345",
                "ContactInformation": {"TelephoneNumberIdentifier": ["12345678", "14"]},
            },
            {
                "EmploymentIdentifier": "54321",
                "ContactInformation": {"TelephoneNumberIdentifier": ["87654321"]},
            },
        ],
    }

    # Act
    sd_base_person = convert_to_sd_base_person(person)

    # Assert
    assert sd_base_person == SDBasePerson(
        cpr="1111111111",
        given_name="Bruce",
        surname="Lee",
        emp_with_telephone_number_identifiers=[
            EmploymentWithTelephoneNumberIdentifier(
                employment_identifier="12345", telephone_number_ids=["12345678", "14"]
            ),
            EmploymentWithTelephoneNumberIdentifier(
                employment_identifier="54321", telephone_number_ids=["87654321"]
            ),
        ],
    )
