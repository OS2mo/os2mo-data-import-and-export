from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import call
from unittest.mock import MagicMock
from uuid import UUID

import hypothesis.strategies as st
from hypothesis import given
from ra_utils.attrdict import attrdict

from integrations.SD_Lon.sd_importer import SdImport
from os2mo_data_import import ImportHelper


class SdImportTest(SdImport):
    def __init__(self, *args, **kwargs):
        self.add_people_mock = MagicMock()
        self.read_department_info_mock = MagicMock()

        self.add_people = self.add_people_mock
        self._read_department_info = self.read_department_info_mock
        super().__init__(*args, **kwargs)


def get_sd_importer(
    municipality_name: str = "Andeby Kommune",
    municipality_code: str = "11223344",
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
        store_integration_data=False,
        seperate_names=True,
    )

    settings = {
        "municipality.name": municipality_name,
        "municipality.code": municipality_code,
        "integrations.SD_Lon.global_from_date": "1970-01-01",
    }
    settings.update(override_settings)
    sd = SdImportTest(
        importer, org_only=org_only, ad_info=None, manager_rows=None, settings=settings
    )

    # add_people should not be called when rg-only
    if org_only:
        sd.add_people_mock.assert_has_calls([])
    else:
        sd.add_people_mock.assert_has_calls([call()])
    sd.read_department_info_mock.assert_has_calls([call()])

    assert sd.importer.organisation is not None
    assert sd.importer.organisation[0] == municipality_name
    assert sd.importer.organisation[1].uuid is None
    assert sd.importer.organisation[1].name == municipality_name
    assert sd.importer.organisation[1].user_key == municipality_name
    assert sd.importer.organisation[1].municipality_code == municipality_code
    assert sd.importer.organisation[1].date_from == "1930-01-01"
    assert sd.importer.organisation[1].date_to == "infinity"
    assert sd.importer.organisation[1].integration_data == {}

    assert sd.importer.klassifikation is not None
    assert sd.importer.klassifikation[0] == municipality_name
    assert sd.importer.klassifikation[1].organisation_uuid is None
    assert sd.importer.klassifikation[1].parent_name == municipality_name
    assert sd.importer.klassifikation[1].user_key == municipality_name
    assert sd.importer.klassifikation[1].description == "umbrella"
    assert sd.importer.klassifikation[1].date_from == "1930-01-01"
    assert sd.importer.klassifikation[1].date_to == "infinity"
    assert sd.importer.klassifikation[1].integration_data == {}

    # 28 classes exist hardcoded within sd_importer.py
    assert len(sd.importer.klasse_objects) == 28

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


@given(st.text(), st.text(), st.booleans())
def test_instantiation(municipality_name: str, municipality_code: str, org_only: bool):
    get_sd_importer(municipality_name, municipality_code, org_only)


@given(st.booleans())
def test_create_employee(create_associations: bool):
    sd = get_sd_importer(
        override_settings={
            "integrations.SD_Lon.monthly_hourly_divide": 9000,
            "integrations.SD_Lon.job_function": "EmploymentName",
            "integrations.SD_Lon.import.too_deep": [],
            "integrations.SD_Lon.sd_importer.create_associations": create_associations,
        }
    )
    sd.nodes["org_unit_uuid"] = attrdict({"name": "org_unit"})

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
    assert engagement_type.integration_data == job_id.integration_data == {}
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
    assert len(sd.importer.organisation_units) == 0
    assert len(sd.importer.organisation_unit_details) == 0

    # But one of these
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
        engagement, association = details

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
    assert engagement.primary_ref == "Ansat"
    assert engagement.org_unit_ref == "org_unit_uuid"
    assert engagement.type_ref == "engagement_typejob_id_123"
    assert engagement.job_function_ref == "job_id_123"
