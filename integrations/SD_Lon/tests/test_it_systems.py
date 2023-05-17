from unittest.mock import MagicMock
from uuid import UUID

import pytest

from sdlon.it_systems import get_sd_to_ad_it_system_uuid, \
    QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID, get_employee_it_systems, \
    QUERY_GET_EMPLOYEE_IT_SYSTEMS


@pytest.fixture
def mock_graphql_client():
    return MagicMock()


def test_get_sd_to_ad_it_system_uuid(mock_graphql_client: MagicMock) -> None:
    # Arrange
    mock_execute = MagicMock(return_value={
        "itsystems": {
            "objects": [
                {
                    "uuid": "988dead8-7564-464a-8339-b7057bfa2665"
                }
            ]
        }
    })
    mock_graphql_client.execute = mock_execute

    # Act
    it_system_uuid = get_sd_to_ad_it_system_uuid(mock_graphql_client)

    # Assert
    mock_execute.assert_called_once_with(QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID)
    assert it_system_uuid == UUID("988dead8-7564-464a-8339-b7057bfa2665")


def test_get_employee_it_systems(mock_graphql_client: MagicMock) -> None:
    # Arrange
    mock_execute = MagicMock(return_value={
        "employees": {
            "objects": [
                {
                    "current": {
                        "itusers": [
                            {
                                "itsystem": {
                                    "uuid": "5168dd45-4cb5-4932-b8a1-10dbe736fc5d"
                                }
                            },
                            {
                                "itsystem": {
                                    "uuid": "a1608e69-c422-404f-a6cc-b873c50af111"
                                }
                            }
                        ]
                    }
                }
            ]
        }
    })
    mock_graphql_client.execute = mock_execute

    # Act
    it_systems = get_employee_it_systems(
        mock_graphql_client,
        UUID("353ed4ae-f489-11ed-bba5-0bfbad9d10d2")
    )

    # Assert
    mock_execute.assert_called_once_with(
        QUERY_GET_EMPLOYEE_IT_SYSTEMS,
        variable_values={"uuid": "353ed4ae-f489-11ed-bba5-0bfbad9d10d2"}
    )
    assert it_systems == [
        UUID("5168dd45-4cb5-4932-b8a1-10dbe736fc5d"),
        UUID("a1608e69-c422-404f-a6cc-b873c50af111")
    ]
