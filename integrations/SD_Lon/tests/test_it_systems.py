from datetime import date
from unittest.mock import MagicMock, patch
from uuid import UUID

from sdlon.models import ITUserSystem
from tests.fixtures import mock_graphql_client

from sdlon.it_systems import (
    get_sd_to_ad_it_system_uuid,
    QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID,
    get_employee_it_systems,
    QUERY_GET_EMPLOYEE_IT_SYSTEMS,
    add_it_system_to_employee,
    MUTATION_ADD_IT_SYSTEM_TO_EMPLOYEE,
)


def test_get_sd_to_ad_it_system_uuid(mock_graphql_client: MagicMock) -> None:
    # Arrange
    mock_execute = MagicMock(
        return_value={
            "itsystems": {"objects": [{"uuid": "988dead8-7564-464a-8339-b7057bfa2665"}]}
        }
    )
    mock_graphql_client.execute = mock_execute

    # Act
    it_system_uuid = get_sd_to_ad_it_system_uuid(
        mock_graphql_client, "AD-bruger til SD"
    )

    # Assert
    mock_execute.assert_called_once_with(
        QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID,
        variable_values={"user_key": "AD-bruger til SD"},
    )
    assert it_system_uuid == UUID("988dead8-7564-464a-8339-b7057bfa2665")


def test_get_employee_it_systems(mock_graphql_client: MagicMock) -> None:
    # Arrange
    mock_execute = MagicMock(
        return_value={
            "employees": {
                "objects": [
                    {
                        "current": {
                            "itusers": [
                                {
                                    "itsystem": {
                                        "uuid": "5168dd45-4cb5-4932-b8a1-10dbe736fc5d"
                                    },
                                    "user_key": "user_key1",
                                },
                                {
                                    "itsystem": {
                                        "uuid": "a1608e69-c422-404f-a6cc-b873c50af111"
                                    },
                                    "user_key": "user_key2",
                                },
                            ]
                        }
                    }
                ]
            }
        }
    )
    mock_graphql_client.execute = mock_execute

    # Act
    it_systems = get_employee_it_systems(
        mock_graphql_client, UUID("353ed4ae-f489-11ed-bba5-0bfbad9d10d2")
    )

    # Assert
    mock_execute.assert_called_once_with(
        QUERY_GET_EMPLOYEE_IT_SYSTEMS,
        variable_values={"uuid": "353ed4ae-f489-11ed-bba5-0bfbad9d10d2"},
    )
    assert it_systems == [
        ITUserSystem(
            uuid=UUID("5168dd45-4cb5-4932-b8a1-10dbe736fc5d"),
            user_key="user_key1",
        ),
        ITUserSystem(
            uuid=UUID("a1608e69-c422-404f-a6cc-b873c50af111"),
            user_key="user_key2",
        ),
    ]


@patch("sdlon.it_systems.date")
def test_add_it_system_to_employee(
    mock_date: MagicMock,
    mock_graphql_client: MagicMock,
) -> None:
    # Arrange
    mock_execute = MagicMock()
    mock_graphql_client.execute = mock_execute
    mock_date.today = MagicMock(return_value=date(2000, 1, 1))

    # Act
    add_it_system_to_employee(
        mock_graphql_client,
        UUID("cfd0b8d2-f48b-11ed-a440-37f1efd1415a"),
        UUID("d0c009f0-f48b-11ed-ba89-ff38216510fc"),
        "AD-bruger fra SD",
    )

    # Assert
    mock_execute.assert_called_once_with(
        MUTATION_ADD_IT_SYSTEM_TO_EMPLOYEE,
        variable_values={
            "input": {
                "user_key": "AD-bruger fra SD",
                "itsystem": "d0c009f0-f48b-11ed-ba89-ff38216510fc",
                "validity": {"from": "2000-01-01"},
                "person": "cfd0b8d2-f48b-11ed-a440-37f1efd1415a",
            }
        },
    )
