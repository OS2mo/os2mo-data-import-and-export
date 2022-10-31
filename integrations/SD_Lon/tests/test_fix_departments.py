from copy import deepcopy
from datetime import date
from datetime import datetime
from typing import Dict
from typing import Optional
from unittest import mock
from unittest import TestCase
from unittest.mock import call
from unittest.mock import MagicMock
from uuid import uuid4, UUID

from os2mo_helpers.mora_helpers import MoraHelper
from parameterized import parameterized

from .test_config import DEFAULT_CHANGED_AT_SETTINGS
from sdlon.config import ChangedAtSettings
from sdlon.fix_departments import FixDepartments


def mock_sd_lookup(service_name, expected_params, response):
    base_responses = {
        "GetDepartment20111201": {
            "Department": [
                {
                    "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                    "DepartmentIdentifier": _TestableFixDepartments.SD_DEPARTMENT_SHORTNAME,
                    "DepartmentName": _TestableFixDepartments.SD_DEPARTMENT_NAME,
                    "ActivationDate": "2019-01-01",
                    "DeactivationDate": "9999-12-31",
                }
            ],
        },
    }
    _response = deepcopy(base_responses[service_name])
    _response.update(response)
    sd_lookup_path = "sdlon.fix_departments.sd_lookup"
    sd_lookup_mock = mock.patch(sd_lookup_path, return_value=_response)
    return sd_lookup_mock


class _TestableFixDepartments(FixDepartments):
    MO_ORG_ROOT = "00000000-0000-0000-0000-000000000000"
    MO_CLASS_USER_KEY = "Enhed"
    MO_CLASS_UUID = str(uuid4())
    SD_INSTITUTION_UUID = str(uuid4())
    SD_DEPARTMENT_NAME = "some department name"
    SD_DEPARTMENT_SHORTNAME = "some department short name"
    SD_DEPARTMENT_PARENT_UUID = str(uuid4())

    @classmethod
    def get_instance(cls, settings_dict: Optional[Dict] = None):
        all_settings_dict = deepcopy(DEFAULT_CHANGED_AT_SETTINGS)
        if settings_dict is not None:
            all_settings_dict.update(settings_dict)
        settings = ChangedAtSettings.parse_obj(all_settings_dict)

        read_mo_org = "sdlon.fix_departments.MoraHelper.read_organisation"
        with mock.patch(read_mo_org, return_value=cls.MO_ORG_ROOT):
            return cls(settings)

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
    alternate_root = str(uuid4())

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
                    "sd_fix_departments_root": alternate_root,
                },
                alternate_root,
            ),
        ]
    )
    def test_root(self, settings, expected_root):
        instance = _TestableFixDepartments.get_instance(settings_dict=settings)
        self.assertEqual(instance.org_uuid, expected_root)


class TestFixDepartment(TestCase):
    def test_run(self):
        instance = _TestableFixDepartments.get_instance()
        instance._create_parent_org_unit_if_missing_in_mo = MagicMock(return_value=None)
        with mock_sd_lookup("GetDepartment20111201", {}, {}):
            instance.fix_department("uuid", date(2020, 1, 1))
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
                        "validity": {"from": "2019-01-01", "to": None},
                    },
                },
            )

    def test_multiple_sd_department_registrations(self):
        instance = _TestableFixDepartments.get_instance()
        instance._create_parent_org_unit_if_missing_in_mo = MagicMock(return_value=None)
        sd_response = {
            "Department": [
                {
                    "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                    "DepartmentIdentifier": _TestableFixDepartments.SD_DEPARTMENT_SHORTNAME,
                    "DepartmentName": _TestableFixDepartments.SD_DEPARTMENT_NAME,
                    "ActivationDate": "2019-01-01",
                    "DeactivationDate": "2023-12-31",
                },
                {
                    "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                    "DepartmentIdentifier": _TestableFixDepartments.SD_DEPARTMENT_SHORTNAME,
                    "DepartmentName": "new name",
                    "ActivationDate": "2024-01-01",
                    "DeactivationDate": "9999-12-31",
                },
            ]
        }
        with mock_sd_lookup("GetDepartment20111201", dict(), sd_response):
            instance.fix_department("uuid", date(2020, 1, 1))

        call_list = instance.helper._mo_post.mock_calls

        first_mo_call = call(
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
                    "validity": {"from": "2019-01-01", "to": "2023-12-31"},
                },
            },
        )
        second_mo_call = call(
            "details/edit",
            {
                "type": "org_unit",
                "data": {
                    "uuid": "uuid",
                    "user_key": instance.SD_DEPARTMENT_SHORTNAME,
                    "name": "new name",
                    "parent": {"uuid": instance.SD_DEPARTMENT_PARENT_UUID},
                    "org_unit_level": {"uuid": instance.MO_CLASS_UUID},
                    "org_unit_type": {"uuid": instance.MO_CLASS_UUID},
                    "validity": {"from": "2024-01-01", "to": None},
                },
            },
        )

        assert first_mo_call in call_list
        assert second_mo_call in call_list

    def test_create_parent_org_unit_return_none_for_sd_root_unit(self):
        # Arrange
        instance = _TestableFixDepartments.get_instance()
        instance.get_parent = MagicMock()
        instance.get_parent.return_value = None

        # Act + Assert
        assert (
            instance._create_parent_org_unit_if_missing_in_mo(
                "unit_uuid", datetime.today()
            )
            is None
        )

    def test_create_parent_org_unit_if_unit_does_not_exists_in_mo(self):
        # Arrange
        instance = _TestableFixDepartments.get_instance()
        parent_parents_uuid = str(uuid4())
        sd_dep_response = {
            "Department": [
                {
                    "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                    "DepartmentIdentifier": "Parent shortname",
                    "DepartmentUUIDIdentifier": _TestableFixDepartments.SD_DEPARTMENT_PARENT_UUID,
                    "DepartmentName": "Parent",
                    "ActivationDate": "2019-01-01",
                    "DeactivationDate": "9999-12-31",
                }
            ]
        }

        with mock_sd_lookup("GetDepartment20111201", dict(), sd_dep_response):
            instance.get_parent = MagicMock(
                side_effect=[
                    _TestableFixDepartments.SD_DEPARTMENT_PARENT_UUID,
                    parent_parents_uuid,
                ]
            )

            instance.helper.read_ou = MagicMock()
            instance.helper.read_ou.return_value = {"status": 404}

            # Act
            expected_uuid = instance._create_parent_org_unit_if_missing_in_mo(
                "unit_uuid", datetime.today()
            )

        # Assert
        assert expected_uuid == UUID(_TestableFixDepartments.SD_DEPARTMENT_PARENT_UUID)

        instance.helper._mo_post.assert_called_once_with(
            "ou/create",
            {
                "uuid": _TestableFixDepartments.SD_DEPARTMENT_PARENT_UUID,
                "user_key": "Parent shortname",
                "name": "Parent",
                "parent": {"uuid": parent_parents_uuid},
                "org_unit_type": {"uuid": _TestableFixDepartments.MO_CLASS_UUID},
                "org_unit_level": {"uuid": _TestableFixDepartments.MO_CLASS_UUID},
                "validity": {
                    "from": "2019-01-01",
                    "to": None,
                },
            },
        )

    def test_fix_department_called_recursively(self):
        # Arrange
        instance = _TestableFixDepartments.get_instance()

        unit_uuid = "11111111-1111-1111-1111-111111111111"
        parent_uuid = "22222222-2222-2222-2222-222222222222"

        instance.get_department = MagicMock(
            side_effect=[
                [
                    {
                        "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                        "DepartmentIdentifier": _TestableFixDepartments.SD_DEPARTMENT_SHORTNAME,
                        "DepartmentUUIDIdentifier": unit_uuid,
                        "DepartmentName": _TestableFixDepartments.SD_DEPARTMENT_NAME,
                        "ActivationDate": "2019-01-01",
                        "DeactivationDate": "9999-12-31",
                    }
                ],
                [
                    {
                        "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
                        "DepartmentIdentifier": "Parent shortname",
                        "DepartmentUUIDIdentifier": parent_uuid,
                        "DepartmentName": "Parent",
                        "ActivationDate": "2019-01-01",
                        "DeactivationDate": "9999-12-31",
                    }
                ],
            ]
        )

        instance._create_parent_org_unit_if_missing_in_mo = MagicMock(
            side_effect=[UUID(parent_uuid), None]
        )
        instance._update_org_unit_for_single_sd_dep_registration = MagicMock()

        # Act
        instance.fix_department(str(unit_uuid), date(2020, 1, 1))

        # Assert
        create_parent_calls = (
            instance._create_parent_org_unit_if_missing_in_mo.mock_calls
        )
        update_calls = (
            instance._update_org_unit_for_single_sd_dep_registration.mock_calls
        )

        assert create_parent_calls == [
            call("11111111-1111-1111-1111-111111111111", datetime(2019, 1, 1, 0, 0)),
            call("22222222-2222-2222-2222-222222222222", datetime(2019, 1, 1, 0, 0)),
        ]

        assert update_calls == [
            call(
                "22222222-2222-2222-2222-222222222222",
                {
                    "DepartmentLevelIdentifier": "Enhed",
                    "DepartmentIdentifier": "Parent shortname",
                    "DepartmentUUIDIdentifier": parent_uuid,
                    "DepartmentName": "Parent",
                    "ActivationDate": "2019-01-01",
                    "DeactivationDate": "9999-12-31",
                },
            ),
            call(
                "11111111-1111-1111-1111-111111111111",
                {
                    "DepartmentLevelIdentifier": "Enhed",
                    "DepartmentIdentifier": "some department short name",
                    "DepartmentUUIDIdentifier": unit_uuid,
                    "DepartmentName": "some department name",
                    "ActivationDate": "2019-01-01",
                    "DeactivationDate": "9999-12-31",
                },
            ),
        ]


def test_lookup_parent_at_correct_effective_date():
    instance = _TestableFixDepartments.get_instance()
    instance.get_parent = MagicMock()

    department = {
        "DepartmentLevelIdentifier": _TestableFixDepartments.MO_CLASS_USER_KEY,
        "DepartmentIdentifier": _TestableFixDepartments.SD_DEPARTMENT_SHORTNAME,
        "DepartmentName": _TestableFixDepartments.SD_DEPARTMENT_NAME,
        "ActivationDate": "2025-01-01",
        "DeactivationDate": "9999-12-31",
    }

    instance._update_org_unit_for_single_sd_dep_registration("unit_uuid", department)

    instance.get_parent.assert_called_once_with("unit_uuid", datetime(2025, 1, 1))
