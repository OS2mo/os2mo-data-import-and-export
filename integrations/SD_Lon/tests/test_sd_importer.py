from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

import hypothesis.strategies as st
import pytest
from hypothesis import given
from hypothesis import settings
from os2mo_data_import import ImportHelper
from os2mo_data_import.mora_data_types import OrganisationUnitType
from parameterized import parameterized
from ra_utils.attrdict import attrdict

from .fixtures import get_department_fixture
from .fixtures import get_organisation_fixture
from sdlon.config import ImporterSettings
from sdlon.sd_importer import SdImport


@pytest.fixture(autouse=True)
def mock_json(monkeypatch):
    monkeypatch.setattr("sdlon.config.load_settings", lambda: dict())


class SdImportTest(SdImport):
    def __init__(self, *args, **kwargs):
        self.add_people_mock = MagicMock()
        self.read_department_info_mock = MagicMock()

        self.orig_add_people = self.add_people
        self.orig_read_department_info = self._read_department_info

        self.add_people = self.add_people_mock
        self._read_department_info = self.read_department_info_mock
        self._get_institution = MagicMock(
            return_value=UUID("00000000-0000-0000-0000-000000000000")
        )

        super().__init__(*args, **kwargs)


def get_sd_importer(
    municipality_name: str = "Andeby Kommune",
    municipality_code: int = 100,
    org_only: bool = False,
    override_settings: Optional[Dict[str, Any]] = None,
) -> SdImportTest:
    override_settings = override_settings or {}

    mora_base = "http://mora/"
    mox_base = "http://mox/"

    importer = ImportHelper(
        create_defaults=True,
        mox_base=mox_base,
        mora_base=mora_base,
        seperate_names=True,
    )

    settings_dict = {
        "municipality_name": municipality_name,
        "municipality_code": municipality_code,
        "sd_global_from_date": "1970-01-01",
        "sd_employment_field": "extension_1",
        "sd_import_run_db": "run_db.sqlite",
        "sd_institution_identifier": "XY",
        "sd_job_function": "EmploymentName",
        "sd_monthly_hourly_divide": 9000,
        "sd_password": "secret",
        "sd_user": "user",
        "sd_importer_create_associations": False,
    }
    settings_dict.update(override_settings)
    settings = ImporterSettings.parse_obj(settings_dict)

    sd = SdImportTest(importer, settings=settings, org_only=org_only, ad_info=None)

    # add_people should not be called when rg-only
    if org_only:
        sd.add_people_mock.assert_not_called()
    else:
        sd.add_people_mock.assert_called_once()
    sd.read_department_info_mock.assert_called_once()

    assert sd.importer.organisation is not None
    assert sd.importer.organisation[0] == municipality_name
    assert sd.importer.organisation[1].uuid == "00000000-0000-0000-0000-000000000000"
    assert sd.importer.organisation[1].name == municipality_name
    assert sd.importer.organisation[1].user_key == municipality_name
    assert sd.importer.organisation[1].municipality_code == str(municipality_code)
    assert sd.importer.organisation[1].date_from == "1930-01-01"
    assert sd.importer.organisation[1].date_to == "infinity"

    assert sd.importer.klassifikation is not None
    assert sd.importer.klassifikation[0] == municipality_name
    assert sd.importer.klassifikation[1].organisation_uuid is None
    assert sd.importer.klassifikation[1].parent_name == municipality_name
    assert sd.importer.klassifikation[1].user_key == municipality_name
    assert sd.importer.klassifikation[1].description == "umbrella"
    assert sd.importer.klassifikation[1].date_from == "1930-01-01"
    assert sd.importer.klassifikation[1].date_to == "infinity"

    # 29 classes exist hardcoded within sd_importer.py
    assert len(sd.importer.klasse_objects) == 30

    # 18 facets in os2mo_data_import/defaults.py
    assert len(sd.importer.facet_objects) == 18

    # No other objects
    assert len(sd.importer.addresses) == 0
    assert len(sd.importer.itsystems) == 0
    assert len(sd.importer.organisation_units) == 0
    assert len(sd.importer.organisation_unit_details) == 0
    assert len(sd.importer.employees) == 0
    assert len(sd.importer.employee_details) == 0

    return sd


@given(st.text(), st.integers(min_value=100, max_value=999), st.booleans())
def test_instantiation(municipality_name: str, municipality_code: int, org_only: bool):
    get_sd_importer(municipality_name, municipality_code, org_only)


@given(st.booleans())
def test_create_employee(create_associations: bool):
    sd = get_sd_importer(
        override_settings={
            "sd_importer_create_associations": create_associations,
        }
    )

    sd.nodes["org_unit_uuid"] = attrdict(
        {"name": "org_unit", "date_from": "1900-01-01"}
    )
    sd.importer.organisation_units["org_unit_uuid"] = OrganisationUnitType(
        name="org_unit",
        type_ref="type_ref",
        date_from="1900-01-01",
    )

    original_classes = set(sd.importer.klasse_objects.keys())

    cpr_no = "0101709999"
    sd.importer.add_employee(
        name=("given_name", "sur_name"),
        identifier=cpr_no,
        cpr_no=cpr_no,
        user_key="employee_user_key",
        uuid="employee_uuid",
    )
    sd.create_employee(
        {
            "PersonCivilRegistrationIdentifier": cpr_no,
            "Employment": [
                {
                    "Profession": {"JobPositionIdentifier": "job_id_123"},
                    "AnniversaryDate": "2004-08-15",
                    "EmploymentStatus": {
                        "EmploymentStatusCode": "1",
                        "ActivationDate": "1970-01-01",
                        "DeactivationDate": "9999-12-31",
                    },
                    "EmploymentIdentifier": "TEST123",
                    "WorkingTime": {"OccupationRate": 1},
                    "EmploymentDepartment": {
                        "DepartmentUUIDIdentifier": "org_unit_uuid",
                    },
                }
            ],
        }
    )

    new_classes = {
        key: value
        for key, value in sd.importer.klasse_objects.items()
        if key not in original_classes
    }
    assert len(new_classes) == 2
    engagement_type = new_classes["engagement_typejob_id_123"]
    job_id = new_classes["job_id_123"]
    assert engagement_type.date_from == job_id.date_from == "1930-01-01"
    assert engagement_type.date_to == job_id.date_to == "infinity"
    assert engagement_type.description == job_id.description is None
    assert engagement_type.scope == job_id.scope == "TEXT"
    assert engagement_type.example == job_id.example is None
    assert engagement_type.organisation_uuid == job_id.organisation_uuid is None
    assert engagement_type.facet_uuid == job_id.facet_uuid is None

    assert engagement_type.user_key == "engagement_typejob_id_123"
    assert engagement_type.title == "job_id_123"
    assert engagement_type.facet_type_ref == "engagement_type"
    UUID(engagement_type.uuid)

    assert job_id.user_key == "job_id_123"
    assert job_id.title == "job_id_123"
    assert job_id.facet_type_ref == "engagement_job_function"
    UUID(job_id.uuid)

    # 18 facets in os2mo_data_import/defaults.py
    assert len(sd.importer.facet_objects) == 18

    # None of these objects
    assert len(sd.importer.addresses) == 0
    assert len(sd.importer.itsystems) == 0
    assert len(sd.importer.organisation_unit_details) == 0

    # But one of these
    assert len(sd.importer.organisation_units) == 1
    assert len(sd.importer.employees) == 1
    employee = sd.importer.employees[cpr_no]
    assert employee.givenname == "given_name"
    assert employee.surname == "sur_name"
    assert employee.cpr_no == cpr_no
    assert employee.user_key == "employee_user_key"

    assert len(sd.importer.employee_details) == 1
    details = sd.importer.employee_details[cpr_no]

    if create_associations:
        # We expect one engagement, and one association
        assert len(details) == 2
        association, engagement = details

        assert association.type_id == "association"
        assert association.date_from == "1970-01-01"
        assert association.date_to is None
        assert association.user_key == "TEST123"
        assert association.org_unit_ref == "org_unit_uuid"
        assert association.type_ref == "SD-medarbejder"
    else:
        # We expect just an engagement
        assert len(details) == 1
        engagement = details[0]

    assert engagement.type_id == "engagement"
    assert engagement.date_from == "1970-01-01"
    assert engagement.date_to is None
    assert engagement.user_key == "TEST123"
    assert engagement.fraction == 1000000
    assert engagement.primary_ref == "non-primary"
    assert engagement.org_unit_ref == "org_unit_uuid"
    assert engagement.type_ref == "engagement_typejob_id_123"
    assert engagement.job_function_ref == "job_id_123"


@patch("sdlon.sd_common.sd_lookup_settings")
@patch("sdlon.sd_common.requests.get")
@patch("integrations.dawa_helper.dawa_lookup")
@given(st.booleans())
@settings(deadline=None)
def test_create_ou_tree(
    dawa_lookup, requests_get, sd_settings, create_email_addresses: bool
):
    sd_settings.return_value = ("", "", "")
    dawa_lookup.return_value = None

    sd = get_sd_importer(
        override_settings={
            "sd_importer_create_email_addresses": create_email_addresses,
        }
    )
    institution_uuid = str(uuid4())
    region_uuid = str(uuid4())
    department1_uuid = str(uuid4())
    sub_department1_uuid = str(uuid4())
    department2_uuid = str(uuid4())
    sub_department2_uuid = str(uuid4())

    original_classes = set(sd.importer.klasse_objects.keys())

    requests_get.return_value = get_department_fixture(
        institution_uuid=institution_uuid,
        region_uuid=region_uuid,
        department1_uuid=department1_uuid,
        sub_department1_uuid=sub_department1_uuid,
        department2_uuid=department2_uuid,
        sub_department2_uuid=sub_department2_uuid,
    )
    sd.info = sd.orig_read_department_info()

    org_unit_level_classes = {
        key: value
        for key, value in sd.importer.klasse_objects.items()
        if key not in original_classes
    }
    assert len(org_unit_level_classes) == 2
    afdelings_niveau = org_unit_level_classes["Afdelings-niveau"]
    ny5_niveau = org_unit_level_classes["NY5-niveau"]

    assert afdelings_niveau.date_from == ny5_niveau.date_from == "1930-01-01"
    assert afdelings_niveau.date_to == ny5_niveau.date_to == "infinity"
    assert afdelings_niveau.description == ny5_niveau.description is None
    assert afdelings_niveau.scope == ny5_niveau.scope == "TEXT"
    assert afdelings_niveau.example == ny5_niveau.example is None
    assert afdelings_niveau.organisation_uuid == ny5_niveau.organisation_uuid is None
    assert afdelings_niveau.facet_uuid == ny5_niveau.facet_uuid is None
    assert (
        afdelings_niveau.facet_type_ref == ny5_niveau.facet_type_ref == "org_unit_level"
    )

    assert afdelings_niveau.user_key == "Afdelings-niveau"
    assert afdelings_niveau.title == "Afdelings-niveau"
    UUID(afdelings_niveau.uuid)

    assert ny5_niveau.user_key == "NY5-niveau"
    assert ny5_niveau.title == "NY5-niveau"
    UUID(ny5_niveau.uuid)

    requests_get.return_value = get_organisation_fixture(
        institution_uuid=institution_uuid,
        region_uuid=region_uuid,
        department1_uuid=department1_uuid,
        sub_department1_uuid=sub_department1_uuid,
        department2_uuid=department2_uuid,
        sub_department2_uuid=sub_department2_uuid,
    )
    sd.create_ou_tree(create_orphan_container=False, sub_tree=None, super_unit=None)

    assert sd.importer.organisation is not None
    assert sd.importer.klassifikation is not None
    assert len(sd.importer.facet_objects) == 18

    # None of these objects
    assert len(sd.importer.addresses) == 0
    assert len(sd.importer.itsystems) == 0
    assert len(sd.importer.employees) == 0
    assert len(sd.importer.employee_details) == 0

    # But some of these
    assert len(sd.importer.organisation_units) == 5
    department1 = sd.importer.organisation_units[department1_uuid]
    sub_department1 = sd.importer.organisation_units[sub_department1_uuid]
    department2 = sd.importer.organisation_units[department2_uuid]
    sub_department2 = sd.importer.organisation_units[sub_department2_uuid]

    assert department1.date_from == "2010-01-01"
    assert department1.date_to is None
    assert department1.type_ref == "Enhed"

    assert sub_department1.date_from == "2011-01-01"
    assert sub_department1.date_to is None
    assert sub_department1.type_ref == "Enhed"

    assert department2.date_from == "2012-01-01"
    assert department2.date_to is None
    assert department2.type_ref == "Enhed"

    assert sub_department2.date_from == "2013-01-01"
    assert sub_department2.date_to is None
    assert sub_department2.type_ref == "Enhed"

    assert department1.name == "D1X-name"
    assert department1.user_key == "D1X"
    assert department1.org_unit_level_ref == "Afdelings-niveau"
    UUID(department1.uuid)

    assert sub_department1.name == "D1Y-name"
    assert sub_department1.user_key == "D1Y"
    assert sub_department1.org_unit_level_ref == "NY5-niveau"
    UUID(sub_department1.uuid)

    assert department2.name == "D2X-name"
    assert department2.user_key == "D2X"
    assert department2.org_unit_level_ref == "Afdelings-niveau"
    UUID(department2.uuid)

    assert sub_department2.name == "D2Y-name"
    assert sub_department2.user_key == "D2Y"
    assert sub_department2.org_unit_level_ref == "NY5-niveau"
    UUID(sub_department2.uuid)

    assert len(sd.importer.organisation_unit_details) == 5
    details = list(sd.importer.organisation_unit_details.values())
    (
        historic_details,
        department1_details,
        sub_department1_details,
        department2_details,
        sub_department2_details,
    ) = details

    assert department1_details == []
    assert department2_details == []

    if not create_email_addresses:
        assert sub_department1_details == []
        assert sub_department2_details == []
    else:
        assert len(sub_department1_details) == 1
        sub_department1_address = sub_department1_details[0]
        assert sub_department1_address.type_id == "address"
        assert sub_department1_address.date_from == "2011-01-01"
        assert sub_department1_address.date_to is None
        assert sub_department1_address.value == "sub_department_1@example.org"
        assert sub_department1_address.type_ref == "EmailUnit"

        assert len(sub_department2_details) == 1
        sub_department2_address = sub_department2_details[0]
        assert sub_department2_address.type_id == "address"
        assert sub_department2_address.date_from == "2013-01-01"
        assert sub_department2_address.date_to is None
        assert sub_department2_address.value == "sub_department_2@example.org"
        assert sub_department2_address.type_ref == "EmailUnit"


@patch("sdlon.sd_importer.uuid.uuid4")
def test_set_engagement_on_leave(mock_uuid4):

    # Arrange

    mock_uuid4.return_value = UUID("00000000-0000-0000-0000-000000000000")
    sd = get_sd_importer()

    sd.nodes["org_unit_uuid"] = attrdict(
        {"name": "org_unit", "date_from": "1900-01-01"}
    )
    sd.importer.organisation_units["org_unit_uuid"] = OrganisationUnitType(
        name="org_unit",
        type_ref="type_ref",
        date_from="1900-01-01",
    )

    cpr_no = "0101709999"
    sd.importer.add_employee(
        name=("given_name", "sur_name"),
        identifier=cpr_no,
        cpr_no=cpr_no,
        user_key="employee_user_key",
        uuid="employee_uuid",
    )

    # Act

    # Create an employee on leave (SD EmploymentStatusCode = 3)
    sd.create_employee(
        {
            "PersonCivilRegistrationIdentifier": cpr_no,
            "Employment": [
                {
                    "EmploymentDate": "1960-01-01",
                    "AnniversaryDate": "2004-08-15",
                    "Profession": {"JobPositionIdentifier": "job_id_123"},
                    "EmploymentStatus": {
                        "EmploymentStatusCode": "3",
                        "ActivationDate": "1970-01-01",
                        "DeactivationDate": "9999-12-31",
                    },
                    "EmploymentIdentifier": "TEST123",
                    "WorkingTime": {"OccupationRate": 1},
                    "EmploymentDepartment": {
                        "DepartmentUUIDIdentifier": "org_unit_uuid",
                    },
                }
            ],
        }
    )

    # Assert

    details = sd.importer.employee_details[cpr_no]
    engagement, leave = details

    assert len(details) == 2
    assert engagement.type_id == "engagement"
    assert leave.type_id == "leave"

    assert engagement.uuid == "00000000-0000-0000-0000-000000000000"
    assert leave.engagement_uuid == "00000000-0000-0000-0000-000000000000"
    assert leave.date_from == "1970-01-01"


@patch("sdlon.sd_importer.uuid.uuid4")
def test_manager_dates_set_correctly(mock_uuid4):
    # Arrange

    mock_uuid4.return_value = UUID("00000000-0000-0000-0000-000000000000")
    sd = get_sd_importer()

    ou = attrdict({"name": "org_unit", "date_from": "1960-01-01"})

    sd.nodes["org_unit_uuid"] = ou
    sd.importer.organisation_units["org_unit_uuid"] = ou

    cpr_no = "0101709999"
    sd.importer.add_employee(
        name=("given_name", "sur_name"),
        identifier=cpr_no,
        cpr_no=cpr_no,
        user_key="employee_user_key",
        uuid="employee_uuid",
    )

    # Act

    # Create an employee who is a manager (job_pos_id is 1030, 1035 or 1040)
    sd.create_employee(
        {
            "PersonCivilRegistrationIdentifier": cpr_no,
            "Employment": [
                {
                    "EmploymentDate": "2022-06-01",
                    "AnniversaryDate": "2004-08-15",
                    "Profession": {
                        "JobPositionIdentifier": "1030",
                        "ActivationDate": "2022-01-01",
                        "DeactivationDate": "2023-12-31",
                    },
                    "EmploymentStatus": {
                        "EmploymentStatusCode": "1",
                        "ActivationDate": "1970-01-01",
                        "DeactivationDate": "9999-12-31",
                    },
                    "EmploymentIdentifier": "TEST123",
                    "WorkingTime": {"OccupationRate": 1},
                    "EmploymentDepartment": {
                        "DepartmentUUIDIdentifier": "org_unit_uuid",
                    },
                }
            ],
        }
    )

    # Assert

    details = sd.importer.employee_details[cpr_no]
    engagement, manager = details

    assert len(details) == 2
    assert engagement.type_id == "engagement"
    assert manager.type_id == "manager"

    assert manager.date_from == "2022-06-01"
    assert manager.date_to == "2023-12-31"


@patch("sdlon.sd_importer.uuid.uuid4")
def test_create_historic_dummy_engagement(mock_uuid4):
    """
    Test that a historic dummy engagement is created if the employee
    engagement start date is older than the corresponding org unit
    start date (see https://redmine.magenta-aps.dk/issues/51898).
    """

    # Arrange

    mock_uuid4.side_effect = ["historic_org_unit_uuid", "engagement_uuid"]
    sd = get_sd_importer()

    sd.nodes["org_unit_uuid"] = attrdict(
        {
            "name": "org_unit",
            "date_from": "2000-01-01",  # Later than 1970-01-01 (see below)
        }
    )

    sd.importer.organisation_units["org_unit_uuid"] = OrganisationUnitType(
        name="org_unit",
        type_ref="type_ref",
        date_from="2000-01-01",  # Later than 1970-01-01 (see below)
    )

    cpr_no = "0101709999"
    sd.importer.add_employee(
        name=("given_name", "sur_name"),
        identifier=cpr_no,
        cpr_no=cpr_no,
        user_key="employee_user_key",
        uuid="employee_uuid",
    )

    # Act

    # Create an employee on leave (SD EmploymentStatusCode = 3)
    sd.create_employee(
        {
            "PersonCivilRegistrationIdentifier": cpr_no,
            "Employment": [
                {
                    "EmploymentDate": "1960-01-01",
                    "AnniversaryDate": "2004-08-15",
                    "Profession": {"JobPositionIdentifier": "job_id_123"},
                    "EmploymentStatus": {
                        "EmploymentStatusCode": "1",
                        "ActivationDate": "1970-01-01",
                        "DeactivationDate": "9999-12-31",
                    },
                    "EmploymentIdentifier": "TEST123",
                    "WorkingTime": {"OccupationRate": 1},
                    "EmploymentDepartment": {
                        "DepartmentUUIDIdentifier": "org_unit_uuid",
                    },
                }
            ],
        }
    )

    # Assert

    details = sd.importer.employee_details[cpr_no]
    engagement, engagement_historic_dummy = details

    assert mock_uuid4.call_count == 2

    # Two engagements are expected - one active and one historic dummy
    assert len(details) == 2

    assert engagement.type_id == "engagement"
    assert engagement_historic_dummy.type_id == "engagement"
    assert engagement.date_from == "2000-01-01"
    assert engagement.date_to is None

    assert engagement_historic_dummy.date_from == "1970-01-01"
    assert engagement_historic_dummy.date_to == "1999-12-31"
    assert engagement_historic_dummy.org_unit_ref == "historic_org_unit_uuid"
    assert engagement_historic_dummy.type_ref == "historisk"


@parameterized.expand([("7",), ("8",), ("9",)])
def test_skip_creating_status_let_go_engagement_when_date_from_older_than_org_unit(
    status_code: str,
):

    # Arrange

    sd = get_sd_importer()

    sd.nodes["org_unit_uuid"] = attrdict(
        {
            "name": "org_unit",
            "date_from": "2022-06-01",  # Later than EmploymentDate (see below)
        }
    )

    sd.importer.organisation_units["org_unit_uuid"] = OrganisationUnitType(
        name="org_unit",
        type_ref="type_ref",
        date_from="2022-06-01",  # Later than EmploymentDate (see below)
    )

    cpr_no = "0101709999"
    sd.importer.add_employee(
        name=("given_name", "sur_name"),
        identifier=cpr_no,
        cpr_no=cpr_no,
        user_key="employee_user_key",
        uuid="employee_uuid",
    )

    sd.importer.add_employee = MagicMock()

    # Act

    sd.create_employee(
        {
            "PersonCivilRegistrationIdentifier": cpr_no,
            "Employment": [
                {
                    "EmploymentDate": "2021-05-01",
                    "AnniversaryDate": "2000-08-15",
                    "Profession": {"JobPositionIdentifier": "job_id_123"},
                    "EmploymentStatus": {
                        "EmploymentStatusCode": status_code,
                        "ActivationDate": "2022-01-01",
                        "DeactivationDate": "9999-12-31",
                    },
                    "EmploymentIdentifier": "TEST123",
                    "WorkingTime": {"OccupationRate": 1},
                    "EmploymentDepartment": {
                        "DepartmentUUIDIdentifier": "org_unit_uuid",
                    },
                }
            ],
        }
    )

    # Assert
    sd.importer.add_employee.assert_not_called()


def test_employment_date_as_engagement_start_date_disabled_per_default():
    sd = get_sd_importer()
    assert sd.employment_date_as_engagement_start_date is False
