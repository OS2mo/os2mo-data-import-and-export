from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch
from uuid import uuid4

from reports.holstebro.manager_report import GET_EMPLOYEE_QUERY
from reports.holstebro.manager_report import GET_ORG_UNITS_QUERY
from reports.holstebro.manager_report import XLSXRow
from reports.holstebro.manager_report import employee_to_xlsx_exporter_format
from reports.holstebro.manager_report import employees_to_xlsx_rows
from reports.holstebro.manager_report import get_employees
from reports.holstebro.manager_report import get_ny_level_org_units
from reports.holstebro.manager_report import get_org_units
from reports.holstebro.manager_report import main
from reports.holstebro.manager_report import ny_level_regex
from reports.holstebro.manager_report import org_units_to_xlsx_exporter_format
from reports.holstebro.manager_report import sd_emp_id_regex
from reports.query_actualstate import XLSXExporter

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
                    "org_unit": [{"uuid": "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b"}],
                }
            ],
            "engagements": [
                {
                    "user_key": "12345",
                    "org_unit": [
                        {
                            "uuid": "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
                            "name": "Vamdrup skole",
                            "user_key": "VAMD",
                            "org_unit_level": {"user_key": "NY3½-niveau"},
                        }
                    ],
                    "is_primary": True,
                },
                {
                    "user_key": "98765",
                    "org_unit": [
                        {
                            "uuid": "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
                            "name": "Magenta DIPEX Department",
                            "user_key": "DIPEX",
                            "org_unit_level": {"user_key": "NY3½-niveau"},
                        },
                    ],
                    "is_primary": False,
                },
                {
                    "user_key": "87654",
                    "org_unit": [
                        {
                            "uuid": "ee557412-6ae4-4008-b811-3d2d7a151cd0",
                            "name": "Another Magenta DIPEX Department",
                            "user_key": "DIPEX2",
                            "org_unit_level": {"user_key": "Afdelings-niveau"},
                        },
                    ],
                    "is_primary": False,
                },
                {
                    "user_key": str(uuid4()),
                    "org_unit": [
                        {
                            "uuid": "5ec16e3d-b08a-4f68-9732-fc5a48a4e887",
                            "name": "Department for manually created engagments",
                            "user_key": "MAN",
                            "org_unit_level": {"user_key": "NY1-niveau"},
                        },
                    ],
                    "is_primary": False,
                },
            ],
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
            "engagements": [],
        }
    },
]

EMPLOYEE_OBJ_BATCH2 = [
    {
        "current": {
            "given_name": "Anna Brink",
            "name": "Anna Brink Nielsen",
            "cpr_number": "0505126786",
            "addresses": [{"name": "annan@kolding.dk"}],
            "manager_roles": [],
            "engagements": [
                {
                    "user_key": "34567",
                    "org_unit": [
                        {
                            "uuid": "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
                            "name": "Vamdrup skole",
                            "user_key": "VAMD",
                            "org_unit_level": {"user_key": "NY3½-niveau"},
                        }
                    ],
                    "is_primary": True,
                },
                {
                    "user_key": "45678",
                    "org_unit": [
                        {
                            "uuid": "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
                            "name": "Magenta DIPEX Department",
                            "user_key": "DIPEX",
                            "org_unit_level": {"user_key": "NY3½-niveau"},
                        }
                    ],
                    "is_primary": False,
                },
            ],
        }
    }
]

OU_BATCH = [
    {
        "current": {
            "name": "Viuf Skole",
            "user_key": "VIUF",
            "uuid": "08eaf849-e9f9-53e0-b6b9-3cd45763ecbb",
            "org_unit_level": {"user_key": "NY3-niveau"},
            "parent": {
                "uuid": "2665d8e0-435b-5bb6-a550-f275692984ef",
                "user_key": "SKOL",
            },
        }
    },
    {
        "current": {
            "name": "Lunderskov Skole",
            "user_key": "LUND",
            "uuid": "09c347ef-451f-5919-8d41-02cc989a6d8b",
            "org_unit_level": {"user_key": "NY3-niveau"},
            "parent": {
                "uuid": "2665d8e0-435b-5bb6-a550-f275692984ef",
                "user_key": "SKOL",
            },
        }
    },
    {
        "current": {
            "name": "Magenta Skole",
            "user_key": "MAG",
            "uuid": "c63d891c-aaf2-4243-bdee-2840f4f55b29",
            "org_unit_level": {"user_key": "Afdelings-niveau"},
            "parent": {
                "uuid": "2665d8e0-435b-5bb6-a550-f275692984ef",
                "user_key": "SKOL",
            },
        }
    },
]

EXPECTED_EMPLOYEE_EXPORTER_DATA_FORMAT = [
    [
        "Medarbejdernummer",
        "Fornavn",
        "Efternavn",
        "Mail",
        "CPR",
        "Afdelingskode",
        "ErLeder",
    ],
    [
        "12345",
        "Birgitta Munk",
        "Duschek",
        "",
        "1212126788",
        "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
        "Ja",
    ],
    [
        "98765",
        "Birgitta Munk",
        "Duschek",
        "",
        "1212126788",
        "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
        "Nej",
    ],
    [
        "34567",
        "Anna Brink",
        "Nielsen",
        "annan@kolding.dk",
        "",
        "5cb38a3c-cacd-5d54-9eb3-88eae2baba1b",
        "Nej",
    ],
    [
        "45678",
        "Anna Brink",
        "Nielsen",
        "annan@kolding.dk",
        "",
        "4aa056ef-e6d2-4ae6-8e86-0ed5a2a567fd",
        "Nej",
    ],
]


def test_get_employees():
    # Arrange
    mock_gql_client = MagicMock()
    mock_gql_client.execute.side_effect = [
        {
            "employees": {
                "page_info": {"next_cursor": "cursor1"},
                "objects": EMPLOYEE_OBJ_BATCH1,
            }
        },
        {
            "employees": {
                "page_info": {"next_cursor": "cursor2"},
                "objects": EMPLOYEE_OBJ_BATCH2,
            }
        },
        {"employees": {"page_info": {"next_cursor": None}, "objects": []}},
    ]

    # Act
    employees = get_employees(mock_gql_client, "EmailEmployee", limit=2)

    # Assert
    assert employees == EMPLOYEE_OBJ_BATCH1 + EMPLOYEE_OBJ_BATCH2
    assert mock_gql_client.execute.call_args_list == [
        call(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": None,
                "limit": 2,
                "email_addr_type_user_key": "EmailEmployee",
            },
        ),
        call(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": "cursor1",
                "limit": 2,
                "email_addr_type_user_key": "EmailEmployee",
            },
        ),
        call(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": "cursor2",
                "limit": 2,
                "email_addr_type_user_key": "EmailEmployee",
            },
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
    assert employee_exporter_data_format == EXPECTED_EMPLOYEE_EXPORTER_DATA_FORMAT


def test_get_org_units():
    # Arrange
    line_mgmt_hierarchy = uuid4()
    mock_gql_client = MagicMock()
    mock_gql_client.execute.return_value = {"org_units": {"objects": OU_BATCH}}

    # Act
    org_units = get_org_units(mock_gql_client, "linjeorg")

    # Assert
    assert org_units == OU_BATCH
    mock_gql_client.execute.assert_called_once_with(
        GET_ORG_UNITS_QUERY, variable_values={"hierarchy_user_key": "linjeorg"}
    )


def test_get_ny_level_org_units():
    # Act
    ny_level_units = get_ny_level_org_units(OU_BATCH)

    # Assert
    assert ny_level_units == OU_BATCH[:-1]


def test_org_units_to_xlsx_exporter_format():
    # Act
    org_unit_exporter_data_format = org_units_to_xlsx_exporter_format(OU_BATCH)

    # Assert
    assert org_unit_exporter_data_format == [
        ["Afdelingskode", "Afdelingsnavn", "Forældreafdelingskode"],
        [
            "08eaf849-e9f9-53e0-b6b9-3cd45763ecbb",
            "Viuf Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef",
        ],
        [
            "09c347ef-451f-5919-8d41-02cc989a6d8b",
            "Lunderskov Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef",
        ],
        [
            "c63d891c-aaf2-4243-bdee-2840f4f55b29",
            "Magenta Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef",
        ],
    ]


@patch("reports.holstebro.manager_report.xlsxwriter.Workbook")
@patch("reports.holstebro.manager_report.file_uploader")
@patch.object(XLSXExporter, "add_sheet")
@patch(
    "reports.holstebro.manager_report.get_settings",
    return_value=MagicMock(
        crontab_AUTH_SERVER="host",
        client_id="client",
        client_secret="secret",
        mora_base="some url",
    ),
)
@patch("reports.holstebro.manager_report.get_mo_client")
def test_main(
    mock_get_mo_client: MagicMock,
    mock_get_settings: MagicMock,
    mock_add_sheet: MagicMock,
    mock_file_uploader: MagicMock,
    mock_workbook: MagicMock,
):
    # Arrange
    mock_gql_client = MagicMock()
    mock_gql_client.execute.side_effect = [
        {
            "employees": {
                "objects": EMPLOYEE_OBJ_BATCH1,
                "page_info": {"next_cursor": "cursor1"},
            },
        },
        {
            "employees": {
                "objects": EMPLOYEE_OBJ_BATCH2,
                "page_info": {"next_cursor": None},
            }
        },
        {"org_units": {"objects": OU_BATCH}},
    ]
    mock_get_mo_client.return_value = mock_gql_client

    settings = mock_get_settings()

    # Act
    main(settings, 22)

    # Assert
    assert len(mock_add_sheet.call_args_list) == 2

    call1 = mock_add_sheet.call_args_list[0]
    assert len(call1.args) == 3
    assert call1.args[1] == "Ledere"
    assert call1.args[2] == EXPECTED_EMPLOYEE_EXPORTER_DATA_FORMAT

    call2 = mock_add_sheet.call_args_list[1]
    assert len(call2.args) == 3
    assert call2.args[1] == "Enheder"
    assert call2.args[2] == [
        ["Afdelingskode", "Afdelingsnavn", "Forældreafdelingskode"],
        [
            "08eaf849-e9f9-53e0-b6b9-3cd45763ecbb",
            "Viuf Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef",
        ],
        [
            "09c347ef-451f-5919-8d41-02cc989a6d8b",
            "Lunderskov Skole",
            "2665d8e0-435b-5bb6-a550-f275692984ef",
        ],
    ]


def test_ny_regex():
    assert ny_level_regex.match("NY1-niveau")
    assert ny_level_regex.match("NY3½-niveau")
    assert not ny_level_regex.match("Afdelings-niveau")
    assert not ny_level_regex.match("Something else")


def test_sd_emp_id_regex():
    assert sd_emp_id_regex.match("12345")
    assert sd_emp_id_regex.match("02345")
    assert not sd_emp_id_regex.match("1234")
    assert not sd_emp_id_regex.match("123456")
    assert not sd_emp_id_regex.match("E2345")
    assert not sd_emp_id_regex.match("ABCDE")
    assert not sd_emp_id_regex.match("-")
    assert not sd_emp_id_regex.match(str(uuid4()))
