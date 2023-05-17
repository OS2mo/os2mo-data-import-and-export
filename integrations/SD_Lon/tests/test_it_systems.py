from unittest.mock import MagicMock
from uuid import UUID

import pytest

from sdlon.it_systems import get_sd_to_ad_it_system_uuid, \
    QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID


@pytest.fixture
def mock_graphql_client():
    return MagicMock()


def test_get_sd_to_ad_it_system_uuid(mock_graphql_client):
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
