from unittest.mock import MagicMock, call
from uuid import UUID, uuid4

from reports.holstebro.manager_report import get_email_addr_type, \
    get_employees, GET_EMPLOYEE_QUERY


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
    # TODO: assert mock called with


def test_get_employees():
    # Arrange
    email_addr_type = uuid4()

    employee_obj_batch1 = [
        {
            "current": {
                "user_key": "BirgittaD",
                "given_name": "Birgitta Munk",
                "name": "Birgitta Munk Duschek",
                "addresses": [],
                "manager_roles": [
                    {
                        "uuid": "f82a969e-9953-40cf-925d-61629ba7139f"
                    }
                ],
                "engagements": []
            }
        },
        {
            "current": {
                "user_key": "MikkelI",
                "given_name": "Mikkel",
                "name": "Mikkel Iversen",
                "addresses": [],
                "manager_roles": [],
                "engagements": []
            }
        }
    ]

    employee_obj_batch2 = [
        {
            "current": {
                "user_key": "AnnaN",
                "given_name": "Anna Brink",
                "name": "Anna Brink Nielsen",
                "addresses": [
                    {
                        "name": "annan@kolding.dk"
                    }
                ],
                "manager_roles": [],
                "engagements": [
                    {
                        "org_unit": [
                            {
                                "uuid": "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
                                "name": "Vamdrup skole",
                                "user_key": "Vamdrup skole"
                            }
                        ],
                        "is_primary": True
                    }
                ]
            }
        }
    ]

    mock_gql_client = MagicMock()
    mock_gql_client.execute.side_effect = [
        {
            "employees": {
                "page_info": {
                    "next_cursor": "cursor1"
                },
                "objects": employee_obj_batch1
            }
        },
        {
            "employees": {
                "page_info": {
                    "next_cursor": "cursor2"
                },
                "objects": employee_obj_batch2
            }
        },
        {
            "employees": {
                "page_info": {
                    "next_cursor": None
                },
                "objects": []
            }
        },
    ]

    # Act
    employees = get_employees(mock_gql_client, email_addr_type, limit=2)

    # Assert
    assert employees == employee_obj_batch1 + employee_obj_batch2
    assert mock_gql_client.execute.call_args_list == [
        call(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": None,
                "limit": 2,
                "email_addr_type": str(email_addr_type)
            }
        ),
        call(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": "cursor1",
                "limit": 2,
                "email_addr_type": str(email_addr_type)
            }
        ),
        call(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": "cursor2",
                "limit": 2,
                "email_addr_type": str(email_addr_type)
            }
        ),
    ]
