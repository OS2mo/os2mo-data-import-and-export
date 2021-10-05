from copy import deepcopy
from datetime import date
from unittest import mock
from unittest import TestCase
from uuid import uuid4

from os2mo_helpers.mora_helpers import MoraHelper
from parameterized import parameterized

from ..fix_departments import FixDepartments


def mock_sd_lookup(service_name, expected_params, response):
    base_responses = {
        "GetDepartment20111201": {
            "Department": [
                {
                    "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                    "DepartmentIdentifier": _TestableFixDepartments.SD_DEPARTMENT_SHORTNAME,
                    "DepartmentName": _TestableFixDepartments.SD_DEPARTMENT_NAME,
                }
            ],
        },
    }
    _response = deepcopy(base_responses[service_name])
    _response.update(response)
    sd_lookup_path = "integrations.SD_Lon.fix_departments.sd_lookup"
    sd_lookup_mock = mock.patch(sd_lookup_path, return_value=_response)
    return sd_lookup_mock


class _TestableFixDepartments(FixDepartments):
    MO_ORG_ROOT = "mo-org-root"
    MO_CLASS_USER_KEY = "Enhed"
    MO_CLASS_UUID = uuid4()
    SD_INSTITUTION_UUID = uuid4()
    SD_DEPARTMENT_NAME = "some department name"
    SD_DEPARTMENT_SHORTNAME = "some department short name"
    SD_DEPARTMENT_PARENT_UUID = uuid4()

    @classmethod
    def get_instance(cls, settings=None):
        load_settings = "integrations.SD_Lon.fix_departments.load_settings"
        read_mo_org = "integrations.SD_Lon.fix_departments.MoraHelper.read_organisation"
        with mock.patch(load_settings, return_value=settings or {}):
            with mock.patch(read_mo_org, return_value=cls.MO_ORG_ROOT):
                return cls()

    def get_institution(self):
        return self.SD_INSTITUTION_UUID

    def get_parent(self, unit_uuid, validity_date):
        return self.SD_DEPARTMENT_PARENT_UUID

    def _get_mora_helper(self, settings):
        mock_helper = mock.MagicMock(spec=MoraHelper)
        mock_helper.read_organisation = mock.Mock(
            return_value=_TestableFixDepartments.MO_ORG_ROOT
        )
        mock_helper.read_classes_in_facet = mock.Mock(
            return_value=[
                [{"user_key": self.MO_CLASS_USER_KEY, "uuid": self.MO_CLASS_UUID}]
            ]
        )
        return mock_helper


class TestFixDepartmentsRootSetting(TestCase):
    alternate_root = "mo-org-root-alternate"

    @parameterized.expand(
        [
            # Case 1: Default root
            (
                {},
                _TestableFixDepartments.MO_ORG_ROOT,
            ),
            # Case 2: Alternate root
            (
                {
                    "integrations.SD_Lon.fix_departments_root": alternate_root,
                },
                alternate_root,
            ),
        ]
    )
    def test_root(self, settings, expected_root):
        instance = _TestableFixDepartments.get_instance(settings=settings)
        self.assertEqual(instance.org_uuid, expected_root)


class TestFixDepartmentAtSingleDate(TestCase):
    def test_run(self):
        instance = _TestableFixDepartments.get_instance()
        with mock_sd_lookup("GetDepartment20111201", {}, {}):
            instance.fix_department_at_single_date("uuid", date(2020, 1, 1))
            instance.helper._mo_post.assert_called_with(
                "details/edit",
                {
                    "type": "org_unit",
                    "data": {
                        "uuid": "uuid",
                        "user_key": instance.SD_DEPARTMENT_SHORTNAME,
                        "name": instance.SD_DEPARTMENT_NAME,
                        "parent": {"uuid": instance.SD_DEPARTMENT_PARENT_UUID},
                        "org_unit_level": {"uuid": instance.MO_CLASS_UUID},
                        "org_unit_type": {"uuid": instance.MO_CLASS_UUID},
                        "validity": {"from": "1930-01-01", "to": None},
                    },
                },
            )
