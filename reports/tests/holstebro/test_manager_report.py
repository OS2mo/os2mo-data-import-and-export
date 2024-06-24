from unittest.mock import MagicMock
from uuid import UUID

from reports.holstebro.manager_report import get_email_addr_type


def test_get_email_addr_type():
    # Arrange
    mock_gql_client = MagicMock()
    mock_gql_client.execute.return_value = {
        "classes": {
            "objects": [
                {
                    "current": {
                        "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2"
                    }
                }
            ]
        }
    }

    # Act
    addr_type_uuid = get_email_addr_type(mock_gql_client)

    # Assert
    assert addr_type_uuid == UUID("f376deb8-4743-4ca6-a047-3241de8fe9d2")
