from unittest.mock import MagicMock
from uuid import UUID

from sdlon.employees import get_employee, QUERY_GET_EMPLOYEE
from sdlon.models import MOBasePerson
from tests.fixtures import mock_graphql_client


def test_get_employee(mock_graphql_client: MagicMock):
    # Arrange
    mock_execute = MagicMock(
        return_value={
            "employees": {
                "objects": [
                    {
                        "current": {
                            "name": "Solveig Kuhlenhenke",
                            "givenname": "Solveig",
                            "surname": "Kuhlenhenke",
                            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
                        }
                    }
                ]
            }
        }
    )
    mock_graphql_client.execute = mock_execute

    # Act
    employee = get_employee(mock_graphql_client, "1111111112")

    # Assert
    mock_execute.assert_called_once_with(
        QUERY_GET_EMPLOYEE, variable_values={"cpr": "1111111112"}
    )
    assert employee == MOBasePerson(
        cpr="1111111112",
        givenname="Solveig",
        surname="Kuhlenhenke",
        name="Solveig Kuhlenhenke",
        uuid=UUID("23d2dfc7-6ceb-47cf-97ed-db6beadcb09b"),
    )


def test_get_employee_none(mock_graphql_client: MagicMock):
    # Arrange
    mock_execute = MagicMock(return_value={"employees": {"objects": []}})
    mock_graphql_client.execute = mock_execute

    # Act
    employee = get_employee(mock_graphql_client, "1111111112")

    # Assert
    mock_execute.assert_called_once_with(
        QUERY_GET_EMPLOYEE, variable_values={"cpr": "1111111112"}
    )
    assert employee is None
