from datetime import date

from sdlon.sdclient.requests import GetEmploymentRequest


def test_to_query_params_convert_to_string_if_not_none():
    # Arrange
    req = GetEmploymentRequest(
        InstitutionIdentifier="XY",
        EffectiveDate=date(2023, 2, 14),
        EmploymentIdentifier="12345"
    )

    # Act
    result = req.to_query_params()

    # Assert
    assert result == {
        "InstitutionIdentifier": "XY",
        "EffectiveDate": "14.02.2023",
        "EmploymentIdentifier": "12345",
        "StatusActiveIndicator": "True",
        "StatusPassiveIndicator": "False",
        "DepartmentIndicator": "False",
        "EmploymentStatusIndicator": "False",
        "ProfessionIndicator": "False",
        "SalaryAgreementIndicator": "False",
        "SalaryCodeGroupIndicator": "False",
        "WorkingTimeIndicator": "False",
        "UUIDIndicator": "False",
    }
