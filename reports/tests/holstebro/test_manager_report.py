from unittest.mock import MagicMock, call
from uuid import UUID, uuid4

from reports.holstebro.manager_report import get_email_addr_type, \
    get_employees, GET_EMPLOYEE_QUERY, GET_EMAIL_ADDR_TYPE_QUERY, \
    employees_to_xlsx_rows, XLSXRow, to_xlsx_exporter_format

EMPLOYEE_OBJ_BATCH1 = [
    {
        "current": {
            "user_key": "12345",
            "given_name": "Birgitta Munk",
            "name": "Birgitta Munk Duschek",
            "addresses": [],
            "manager_roles": [
                {
                    "uuid": "f82a969e-9953-40cf-925d-61629ba7139f",
                    "org_unit": [
                        {
                            "uuid": "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b"
                        }
                    ]
                }
            ],
            "engagements": [
                {
                    "org_unit": [
                        {
                            "uuid": "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
                            "name": "Vamdrup skole",
                            "user_key": "VAMD"
                        }
                    ],
                    "is_primary": True
                },
                {
                    "org_unit": [
                        {
                            "uuid": "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
                            "name": "Magenta DIPEX Department",
                            "user_key": "DIPEX"
                        }
                    ],
                    "is_primary": False
                },
            ]
        }
    },
    {
        "current": {
            "user_key": "23456",
            "given_name": "Mikkel",
            "name": "Mikkel Iversen",
            "addresses": [],
            "manager_roles": [],
            "engagements": []
        }
    }
]

EMPLOYEE_OBJ_BATCH2 = [
    {
        "current": {
            "user_key": "34567",
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
                            "user_key": "VAMD"
                        }
                    ],
                    "is_primary": True
                },
                {
                    "org_unit": [
                        {
                            "uuid": "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
                            "name": "Magenta DIPEX Department",
                            "user_key": "DIPEX"
                        }
                    ],
                    "is_primary": False
                },
            ]
        }
    }
]


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
    mock_gql_client.execute.assert_called_once_with(GET_EMAIL_ADDR_TYPE_QUERY)


def test_get_employees():
    # Arrange
    email_addr_type = uuid4()

    mock_gql_client = MagicMock()
    mock_gql_client.execute.side_effect = [
        {
            "employees": {
                "page_info": {
                    "next_cursor": "cursor1"
                },
                "objects": EMPLOYEE_OBJ_BATCH1
            }
        },
        {
            "employees": {
                "page_info": {
                    "next_cursor": "cursor2"
                },
                "objects": EMPLOYEE_OBJ_BATCH2
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
    assert employees == EMPLOYEE_OBJ_BATCH1 + EMPLOYEE_OBJ_BATCH2
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


def test_employees_to_xlsx_rows():
    # Act
    xlsx_rows = employees_to_xlsx_rows(EMPLOYEE_OBJ_BATCH1 + EMPLOYEE_OBJ_BATCH2)

    # Assert
    assert xlsx_rows == [
        XLSXRow(
            employment_id="12345",
            first_name="Birgitta Munk",
            last_name="Duschek",
            email=None,
            org_unit_user_key="VAMD",
            is_manager=True,
        ),
        XLSXRow(
            employment_id="12345",
            first_name="Birgitta Munk",
            last_name="Duschek",
            email=None,
            org_unit_user_key="DIPEX",
            is_manager=False,
        ),
        XLSXRow(
            employment_id="34567",
            first_name="Anna Brink",
            last_name="Nielsen",
            email="annan@kolding.dk",
            org_unit_user_key="VAMD",
            is_manager=False,
        ),
        XLSXRow(
            employment_id="34567",
            first_name="Anna Brink",
            last_name="Nielsen",
            email="annan@kolding.dk",
            org_unit_user_key="DIPEX",
            is_manager=False,
        ),
    ]


def test_to_xlsx_exporter_format():
    # Arrange
    xlsx_rows = employees_to_xlsx_rows(EMPLOYEE_OBJ_BATCH1 + EMPLOYEE_OBJ_BATCH2)

    # Act
    exporter_data_format = to_xlsx_exporter_format(xlsx_rows)

    # Assert
    assert exporter_data_format == [
        [
            "Medarbejdernummer",
            "Fornavn",
            "Efternavn",
            "Mail",
            "Afdelingskode",
            "ErLeder"
        ],
        ["12345", "Birgitta Munk", "Duschek", "", "VAMD", "Ja"],
        ["12345", "Birgitta Munk", "Duschek", "", "DIPEX", "Nej"],
        ["34567", "Anna Brink", "Nielsen", "annan@kolding.dk", "VAMD", "Nej"],
        ["34567", "Anna Brink", "Nielsen", "annan@kolding.dk", "DIPEX", "Nej"],
    ]
