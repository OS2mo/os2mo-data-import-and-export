from unittest.mock import MagicMock, call
from uuid import UUID, uuid4

from reports.holstebro.manager_report import get_class_uuid, \
    get_employees, GET_EMPLOYEE_QUERY, GET_CLASS_UUID_QUERY, \
    employees_to_xlsx_rows, XLSXRow, employee_to_xlsx_exporter_format, \
    get_org_units, \
    GET_ORG_UNITS_QUERY, org_units_to_xlsx_exporter_format

EMPLOYEE_OBJ_BATCH1 = [
    {
        "current": {
            "given_name": "Birgitta Munk",
            "name": "Birgitta Munk Duschek",
            "cpr_number": "1212126788",
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
                    "user_key": "12345",
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
                    "user_key": "98765",
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
            "cpr_number": "2201126789",
            "addresses": [],
            "manager_roles": [],
            "engagements": []
        }
    }
]

EMPLOYEE_OBJ_BATCH2 = [
    {
        "current": {
            "given_name": "Anna Brink",
            "name": "Anna Brink Nielsen",
            "cpr_number": "0505126786",
            "addresses": [
                {
                    "name": "annan@kolding.dk"
                }
            ],
            "manager_roles": [],
            "engagements": [
                {
                    "user_key": "34567",
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
                    "user_key": "45678",
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

OU_BATCH = [
    {
        "current": {
            "name": "Viuf Skole",
            "user_key": "VIUF",
            "uuid": "08eaf849-e9f9-53e0-b6b9-3cd45763ecbb",
            "parent": {
                "uuid": "2665d8e0-435b-5bb6-a550-f275692984ef",
                "user_key": "SKOL"
            }
        }
    },
    {
        "current": {
            "name": "Lunderskov Skole",
            "user_key": "LUND",
            "uuid": "09c347ef-451f-5919-8d41-02cc989a6d8b",
            "parent": {
                "uuid": "2665d8e0-435b-5bb6-a550-f275692984ef",
                "user_key": "SKOL"
            }
        }
    },
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
    addr_type_uuid = get_class_uuid(mock_gql_client, "EmailEmployee")

    # Assert
    assert addr_type_uuid == UUID("f376deb8-4743-4ca6-a047-3241de8fe9d2")
    mock_gql_client.execute.assert_called_once_with(
        GET_CLASS_UUID_QUERY,
        variable_values={"user_key": "EmailEmployee"},
    )


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
            email="",
            cpr="1212126788",
            org_unit_uuid="5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
            is_manager=True,
        ),
        XLSXRow(
            employment_id="98765",
            first_name="Birgitta Munk",
            last_name="Duschek",
            email="",
            cpr="1212126788",
            org_unit_uuid="4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
            is_manager=False,
        ),
        XLSXRow(
            employment_id="34567",
            first_name="Anna Brink",
            last_name="Nielsen",
            email="annan@kolding.dk",
            cpr="",
            org_unit_uuid="5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
            is_manager=False,
        ),
        XLSXRow(
            employment_id="45678",
            first_name="Anna Brink",
            last_name="Nielsen",
            email="annan@kolding.dk",
            cpr="",
            org_unit_uuid="4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
            is_manager=False,
        ),
    ]


def test_to_xlsx_exporter_format():
    # Arrange
    xlsx_rows = employees_to_xlsx_rows(EMPLOYEE_OBJ_BATCH1 + EMPLOYEE_OBJ_BATCH2)

    # Act
    employee_exporter_data_format = employee_to_xlsx_exporter_format(xlsx_rows)

    # Assert
    assert employee_exporter_data_format == [
        [
            "Medarbejdernummer",
            "Fornavn",
            "Efternavn",
            "Mail",
            "CPR",
            "Afdelingskode",
            "ErLeder"
        ],
        [
            "12345",
            "Birgitta Munk",
            "Duschek",
            "",
            "1212126788",
            "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
            "Ja"
        ],
        [
            "98765",
            "Birgitta Munk",
            "Duschek",
            "",
            "1212126788",
            "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
            "Nej"
        ],
        [
            "34567",
            "Anna Brink",
            "Nielsen",
            "annan@kolding.dk",
            "",
            "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
            "Nej"
        ],
        [
            "45678",
            "Anna Brink",
            "Nielsen",
            "annan@kolding.dk",
            "",
            "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
            "Nej"
        ],
    ]


def test_get_org_units():
    # Arrange
    mock_gql_client = MagicMock()
    mock_gql_client.execute.return_value = {
        "org_units": {
            "objects": OU_BATCH
        }
    }

    # Act
    org_units = get_org_units(mock_gql_client)

    # Assert
    assert org_units == OU_BATCH
    mock_gql_client.execute.assert_called_once_with(GET_ORG_UNITS_QUERY)


def test_org_units_to_xlsx_exporter_format():
    # Act
    org_unit_exporter_data_format = org_units_to_xlsx_exporter_format(OU_BATCH)

    # Assert
    assert org_unit_exporter_data_format == [
        ["Afdelingskode", "Afdelingsnavn", "Forældreafdelingskode"],
        [
            "08eaf849-e9f9-53e0-b6b9-3cd45763ecbb",
            "Viuf Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef"
        ],
        [
            "09c347ef-451f-5919-8d41-02cc989a6d8b",
            "Lunderskov Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef"
        ]
    ]
