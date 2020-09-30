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
            "integrations.SD_Lon.no_salary_minimum_id": 5000,
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
    def test_construct_object(self, *args):
        self._setup_change_at_sd(clazz=ChangeAtSDTestSubclass)

    @parameterized.expand([
        # job_identifier is compared with no_salary_minimum_id
        ["200", False],
        ["4999", False],
        ["5000", True],
        ["5001", True],
        ["10000", True],
    ])
    @patch('integrations.SD_Lon.sd_changed_at.sd_payloads', autospec=True)
    @patch('integrations.SD_Lon.sd_changed_at.primary_types')
    @patch('integrations.SD_Lon.sd_changed_at.MoraHelper', autospec=True)
    @patch('integrations.SD_Lon.sd_changed_at.MOPrimaryEngagementUpdater')
    @patch('integrations.SD_Lon.sd_changed_at.FixDepartments')
    def test_construct_object(self, job_position_identifier, expected, *args):
        _, _, mora_helper_mock, _, sd_payloads_mock = args

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
                "JobPositionIdentifier": job_position_identifier
            }],
        }
        status = {
            "ActivationDate": "",
            "DeactivationDate": "",
            "EmploymentStatusCode": "",
        }
        cpr = ""
        result = changed_at.create_new_engagement(engagement, status, cpr)
        if expected:
            self.assertTrue(result)
            sd_payloads_mock.create_engagement.assert_called_once()
        else:
            self.assertIsNone(result)
            sd_payloads_mock.create_engagement.assert_not_called()
