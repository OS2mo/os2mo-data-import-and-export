import unittest
from datetime import datetime

from parameterized import parameterized
from unittest.mock import patch, MagicMock

from integrations.SD_Lon.sd_changed_at import ChangeAtSD
from integrations.SD_Lon.exceptions import JobfunctionSettingsIsWrongException


class ChangeAtSDTestSubclass(ChangeAtSD):
    def _load_forced_uuids(self):
        return {}

    def _update_professions(self, job_function):
        return None


class Tests(unittest.TestCase):
    maxDiff = None

    def _setup_change_at_sd(self, update_dict=None, from_date=None, clazz=None):
        # Load defaults
        update_dict = update_dict or {}
        from_date = from_date or datetime(2019, 7, 1, 0, 0)
        clazz = clazz or ChangeAtSD
        # Prepare settings
        settings = self.settings
        settings.update(update_dict)
        return clazz(from_date, settings=settings)

    def setUp(self):
        self.settings = {
            "integrations.SD_Lon.job_function": "EmploymentName",
            "integrations.SD_Lon.use_ad_integration": False,
            "mora.base": "",
            "mox.base": "",
            "integrations.SD_Lon.import.too_deep": "",
            "integrations.SD_Lon.monthly_hourly_divide": 80000,
            "integrations.SD_Lon.no_salary_minimum_id": 9000,
        }

    @parameterized.expand([
        ["", JobfunctionSettingsIsWrongException],
        ["hej", JobfunctionSettingsIsWrongException],
        ["MagicKey", JobfunctionSettingsIsWrongException],

        ["JobPositionIdentifier", Exception],
        ["EmploymentName", Exception],
    ])
    def test_job_function_configuration(self, value, expected_exception):
        with self.assertRaises(expected_exception):
            self._setup_change_at_sd({
                "integrations.SD_Lon.job_function": value
            })

    @patch('integrations.SD_Lon.sd_changed_at.primary_types')
    @patch('integrations.SD_Lon.sd_changed_at.MoraHelper')
    @patch('integrations.SD_Lon.sd_changed_at.MOPrimaryEngagementUpdater')
    @patch('integrations.SD_Lon.sd_changed_at.FixDepartments')
    def test_construct_object(self, fix_dep_mock, mo_primary_mock, mora_helper_mock, primary_types_mock):
        self._setup_change_at_sd(clazz=ChangeAtSDTestSubclass)

    @patch('integrations.SD_Lon.sd_changed_at.sd_payloads', autospec=True)
    @patch('integrations.SD_Lon.sd_changed_at.primary_types')
    @patch('integrations.SD_Lon.sd_changed_at.MoraHelper', autospec=True)
    @patch('integrations.SD_Lon.sd_changed_at.MOPrimaryEngagementUpdater')
    @patch('integrations.SD_Lon.sd_changed_at.FixDepartments')
    def test_construct_object(self, fix_dep_mock, mo_primary_mock, mora_helper_mock, primary_types_mock, sd_payloads_mock):
        mora_helper_mock.return_value.read_ou.return_value = {
            "org_unit_level": {
                "user_key": "IHaveNoIdea",
            },
            "uuid": "uuid-a",
        }
        response = MagicMock()
        response.status_code = 201
        mora_helper_mock.return_value._mo_post.return_value = response

        changed_at = self._setup_change_at_sd(clazz=ChangeAtSDTestSubclass)
        changed_at.mo_person = {
            "uuid": "uuid-b",
        }
        engagement = {
            "EmploymentIdentifier": "BIGAL",
            "EmploymentDepartment": [{
                "DepartmentUUIDIdentifier": "uuid-c"
            }],
            "Profession": [{
                "JobPositionIdentifier": 200
            }],
        }
        status = {
            "ActivationDate": "",
            "DeactivationDate": "",
            "EmploymentStatusCode": "",
        }
        cpr = ""
        changed_at.create_new_engagement(engagement, status, cpr)

        sd_payloads_mock.create_engagement.assert_called_once()
