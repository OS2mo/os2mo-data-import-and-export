import unittest
import uuid
from collections import OrderedDict
from datetime import date
from datetime import datetime
from datetime import timedelta
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

import hypothesis.strategies as st
from hypothesis import example
from hypothesis import given
from parameterized import parameterized
from ra_utils.attrdict import attrdict
from ra_utils.generate_uuid import uuid_generator

from .fixtures import get_employment_fixture
from .fixtures import get_sd_person_fixture
from .fixtures import read_employment_fixture
from integrations.SD_Lon.convert import sd_to_mo_termination_date
from integrations.SD_Lon.exceptions import JobfunctionSettingsIsWrongException
from integrations.SD_Lon.sd_changed_at import ChangeAtSD
from integrations.SD_Lon.sd_changed_at import gen_date_pairs
from test_case import DipexTestCase


class ChangeAtSDTest(ChangeAtSD):
    def __init__(self, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"
        self.primary_types_mock = MagicMock()
        self.primary_engagement_mock = MagicMock()
        self.fix_departments_mock = MagicMock()

        self._get_job_sync = MagicMock()

        self._create_class = MagicMock()
        self._create_class.return_value = "new_class_uuid"

        super().__init__(*args, **kwargs)

    def _get_primary_types(self, mora_helper):
        return self.primary_types_mock

    def _get_primary_engagement_updater(self):
        return self.primary_engagement_mock

    def _get_fix_departments(self):
        return self.fix_departments_mock

    def _read_forced_uuids(self):
        return {}

    def _get_mora_helper(self, mora_base):
        return self.morahelper_mock


def setup_sd_changed_at(updates=None):
    # TODO: remove integrations.SD_Lon.terminate_engagement_with_to_only
    settings = {
        "integrations.SD_Lon.job_function": "JobPositionIdentifier",
        "integrations.SD_Lon.use_ad_integration": False,
        "integrations.SD_Lon.monthly_hourly_divide": 8000,
        "mora.base": "dummy",
        "mox.base": "dummy",
        "integrations.SD_Lon.terminate_engagement_with_to_only": False,
    }
    if updates:
        settings.update(updates)

    today = date.today()
    start_date = today

    sd_updater = ChangeAtSDTest(start_date, start_date + timedelta(days=1), settings)

    return sd_updater


class Test_sd_changed_at(DipexTestCase):
    @patch("integrations.SD_Lon.sd_common.sd_lookup_settings")
    @patch("integrations.SD_Lon.sd_common._sd_request")
    def test_get_sd_person(self, sd_request, sd_settings):
        """Test that read_person does the expected transformation."""
        sd_settings.return_value = ("", "", "")

        cpr = "0101709999"
        sd_reply, expected_read_person_result = get_sd_person_fixture(
            cpr=cpr, first_name="John", last_name="Deere", employment_id="01337"
        )

        sd_request.return_value = sd_reply

        sd_updater = setup_sd_changed_at()
        result = sd_updater.get_sd_person(cpr=cpr)
        self.assertEqual(result, expected_read_person_result)

    def test_update_changed_persons(self):

        cpr = "0101709999"
        first_name = "John"
        last_name = "Deere"

        _, read_person_result = get_sd_person_fixture(
            cpr=cpr,
            first_name=first_name,
            last_name=last_name,
            employment_id="01337",
        )

        sd_updater = setup_sd_changed_at()
        sd_updater.get_sd_person = lambda cpr: read_person_result

        generate_uuid = uuid_generator("test")
        org_uuid = str(generate_uuid("org_uuid"))
        user_uuid = str(generate_uuid("user_uuid"))

        sd_updater.org_uuid = org_uuid

        morahelper = sd_updater.morahelper_mock
        morahelper.read_user.return_value = {
            "uuid": user_uuid,
            "name": " ".join(["Old firstname", last_name]),
            "first_name": "Old firstname",
            "surname": last_name,
        }

        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict(
            {"status_code": 201, "json": lambda: user_uuid}
        )
        self.assertFalse(_mo_post.called)
        sd_updater.update_changed_persons(in_cpr=cpr)
        _mo_post.assert_called_with(
            "e/create",
            {
                "type": "employee",
                "givenname": first_name,
                "surname": last_name,
                "cpr_no": cpr,
                "org": {"uuid": org_uuid},
                "uuid": user_uuid,
                "user_key": user_uuid,
            },
        )

    @given(status=st.sampled_from(["1", "S"]))
    @patch("integrations.SD_Lon.sd_common.sd_lookup_settings")
    @patch("integrations.SD_Lon.sd_common._sd_request")
    def test_read_employment_changed(self, sd_request, sd_settings, status):
        sd_settings.return_value = ("", "", "")

        sd_reply, expected_read_employment_result = read_employment_fixture(
            cpr="0101709999",
            employment_id="01337",
            job_id="1234",
            job_title="EDB-Mand",
            status=status,
        )

        sd_request.return_value = sd_reply
        sd_updater = setup_sd_changed_at()
        result = sd_updater.read_employment_changed()
        self.assertEqual(result, expected_read_employment_result)

    @given(status=st.sampled_from(["1", "S"]))
    def test_update_all_employments(self, status):

        cpr = "0101709999"
        employment_id = "01337"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id=employment_id,
            job_id="1234",
            job_title="EDB-Mand",
            status=status,
        )

        sd_updater = setup_sd_changed_at()
        sd_updater.read_employment_changed = lambda: read_employment_result

        morahelper = sd_updater.morahelper_mock
        morahelper.read_user.return_value.__getitem__.return_value = "user_uuid"

        if status == "1":  # Creates call create engagement, if no eng exists
            sd_updater.create_new_engagement = MagicMock()

            engagement = read_employment_result[0]["Employment"]
            # First employment status entry from read_employment_result
            status = read_employment_result[0]["Employment"]["EmploymentStatus"][0]

            self.assertFalse(sd_updater.create_new_engagement.called)
            sd_updater.update_all_employments()
            sd_updater.create_new_engagement.assert_called_with(
                engagement, status, cpr, "user_uuid"
            )
        elif status == "S":  # Deletes call terminante engagement
            morahelper.read_user_engagement.return_value = [{"user_key": employment_id}]
            sd_updater._terminate_engagement = MagicMock()

            status = read_employment_result[0]["Employment"]["EmploymentStatus"]

            self.assertFalse(sd_updater._terminate_engagement.called)
            sd_updater.update_all_employments()
            sd_updater._terminate_engagement.assert_called_with(
                user_key=employment_id,
                person_uuid="user_uuid",
                from_date=sd_to_mo_termination_date(status["ActivationDate"]),
            )

    @parameterized.expand(
        [
            ["07777", "monthly pay"],
            ["90000", "hourly pay"],
            ["C3-P0", "employment pay"],
        ]
    )
    def test_create_new_engagement(self, employment_id, engagement_type):

        cpr = "0101709999"
        job_id = "1234"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id=employment_id,
            job_id=job_id,
            job_title="EDB-Mand",
        )

        sd_updater = setup_sd_changed_at()

        sd_updater.engagement_types = {
            "månedsløn": "monthly pay",
            "timeløn": "hourly pay",
            "engagement_type" + job_id: "employment pay",
        }

        morahelper = sd_updater.morahelper_mock

        # Load noop NY logic
        sd_updater.apply_NY_logic = (
            lambda org_unit, user_key, validity, person_uuid: org_unit
        )
        # Set primary types
        sd_updater.primary_types = {
            "primary": "primary_uuid",
            "non_primary": "non_primary_uuid",
            "no_salary": "no_salary_uuid",
            "fixed_primary": "fixed_primary_uuid",
        }

        engagement = read_employment_result[0]["Employment"]
        # First employment status entry from read_employment_result
        status = read_employment_result[0]["Employment"]["EmploymentStatus"][0]

        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict(
            {
                "status_code": 201,
            }
        )
        self.assertFalse(_mo_post.called)

        sd_updater._create_engagement_type = MagicMock()
        sd_updater._create_engagement_type.return_value = "new_engagement_type_uuid"

        sd_updater._create_professions = MagicMock()
        sd_updater._create_professions.return_value = "new_profession_uuid"

        sd_updater.create_new_engagement(engagement, status, cpr, "user_uuid")
        _mo_post.assert_called_with(
            "details/create",
            {
                "type": "engagement",
                "org_unit": {"uuid": "department_uuid"},
                "person": {"uuid": "user_uuid"},
                "job_function": {"uuid": "new_profession_uuid"},
                "primary": {"uuid": "non_primary_uuid"},
                "engagement_type": {"uuid": engagement_type},
                "user_key": employment_id,
                "fraction": 0,
                "validity": {"from": "2020-11-10", "to": "2021-02-09"},
            },
        )
        sd_updater._create_engagement_type.assert_not_called()
        sd_updater._create_professions.assert_called_once()

    def test_terminate_engagement(self):

        employment_id = "01337"

        sd_updater = setup_sd_changed_at()
        morahelper = sd_updater.morahelper_mock

        sd_updater.mo_engagements_cache["user_uuid"] = [
            {
                "user_key": employment_id,
                "uuid": "mo_engagement_uuid",
            }
        ]

        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict(
            {"status_code": 200, "text": lambda: "mo_engagement_uuid"}
        )
        self.assertFalse(_mo_post.called)
        sd_updater._terminate_engagement(
            user_key=employment_id, person_uuid="user_uuid", from_date="2020-11-01"
        )
        _mo_post.assert_called_once_with(
            "details/terminate",
            {
                "type": "engagement",
                "uuid": "mo_engagement_uuid",
                "validity": {"from": "2020-11-01", "to": None},
            },
        )

    def test_terminate_engagement_returns_false_when_no_mo_engagement(self):
        sd_updater = setup_sd_changed_at()

        morahelper = sd_updater.morahelper_mock
        mock_read_user_engagement = morahelper.read_user_engagement
        mock_read_user_engagement.return_value = []

        self.assertFalse(
            sd_updater._terminate_engagement(
                user_key="12345", person_uuid=str(uuid.uuid4()), from_date="2021-10-05"
            )
        )

    def test_terminate_engagement_when_to_date_set(self):
        sd_updater = setup_sd_changed_at()
        sd_updater.mo_engagements_cache["person_uuid"] = [
            {
                "user_key": "00000",
                "uuid": "mo_engagement_uuid",
            }
        ]

        morahelper = sd_updater.morahelper_mock
        mock_post_mo = morahelper._mo_post
        mock_post_mo.return_value = attrdict(
            {"status_code": 200, "text": lambda: "mo_engagement_uuid"}
        )

        sd_updater._terminate_engagement(
            user_key="00000",
            person_uuid="person_uuid",
            from_date="2021-10-15",
            to_date="2021-10-20",
        )
        mock_post_mo.assert_called_once_with(
            "details/terminate",
            {
                "type": "engagement",
                "uuid": "mo_engagement_uuid",
                "validity": {"from": "2021-10-15", "to": "2021-10-20"},
            },
        )

    @parameterized.expand([("2021-10-15", "2021-10-14"), ("9999-12-31", None)])
    def test_handle_status_changes_terminates_let_go_employment_status(
        self, sd_deactivation_date, mo_termination_to_date
    ):
        cpr = "0101709999"
        employment_id = "01337"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id=employment_id,
            job_id="1234",
            job_title="EDB-Mand",
            status="1",
        )
        sd_employment = read_employment_result[0]["Employment"]
        sd_employment["EmploymentStatus"].pop(0)  # Remove the one with status 8
        sd_employment["EmploymentStatus"][0]["DeactivationDate"] = sd_deactivation_date

        sd_updater = setup_sd_changed_at()
        sd_updater.mo_engagements_cache["person_uuid"] = [
            {
                "user_key": employment_id,
                "uuid": "mo_engagement_uuid",
            }
        ]

        morahelper = sd_updater.morahelper_mock
        mock_mo_post = morahelper._mo_post
        mock_mo_post.return_value = attrdict(
            {"status_code": 200, "text": lambda: "mo_engagement_uuid"}
        )

        sd_updater._handle_employment_status_changes(
            cpr=cpr, sd_employment=sd_employment, person_uuid="person_uuid"
        )

        mock_mo_post.assert_called_once_with(
            "details/terminate",
            {
                "type": "engagement",
                "uuid": "mo_engagement_uuid",
                "validity": {"from": "2021-02-09", "to": mo_termination_to_date},
            },
        )

    def test_handle_status_changes_terminates_slettet_employment_status(self):
        cpr = "0101709999"
        employment_id = "01337"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id=employment_id,
            job_id="1234",
            job_title="EDB-Mand",
            status="S",
        )
        sd_employment = read_employment_result[0]["Employment"]

        sd_updater = setup_sd_changed_at()
        sd_updater.mo_engagements_cache["person_uuid"] = [
            {
                "user_key": employment_id,
                "uuid": "mo_engagement_uuid",
            }
        ]

        morahelper = sd_updater.morahelper_mock
        mock_mo_post = morahelper._mo_post
        mock_mo_post.return_value = attrdict(
            {"status_code": 200, "text": lambda: "mo_engagement_uuid"}
        )

        sd_updater._handle_employment_status_changes(
            cpr=cpr, sd_employment=sd_employment, person_uuid="person_uuid"
        )

        mock_mo_post.assert_called_once_with(
            "details/terminate",
            {
                "type": "engagement",
                "uuid": "mo_engagement_uuid",
                "validity": {"from": "2020-10-31", "to": None},
            },
        )

    @parameterized.expand(
        [
            ["07777", "monthly pay"],
            ["90000", "hourly pay"],
            ["C3-P0", "employment pay"],
        ]
    )
    def test_update_all_employments_editing(self, employment_id, engagement_type):

        cpr = "0101709999"
        job_id = "1234"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id=employment_id,
            job_id=job_id,
            job_title="EDB-Mand",
        )

        sd_updater = setup_sd_changed_at()
        sd_updater.engagement_types = {
            "månedsløn": "monthly pay",
            "timeløn": "hourly pay",
            "engagement_type" + job_id: "employment pay",
        }

        sd_updater.read_employment_changed = lambda: read_employment_result
        # Load noop NY logic
        sd_updater.apply_NY_logic = (
            lambda org_unit, user_key, validity, person_uuid: org_unit
        )

        morahelper = sd_updater.morahelper_mock
        morahelper.read_user.return_value.__getitem__.return_value = "user_uuid"
        morahelper.read_user_engagement.return_value = [
            {
                "user_key": employment_id,
                "uuid": "mo_engagement_uuid",
                "validity": {"to": "9999-12-31"},
            }
        ]

        # Set primary types
        sd_updater.primary_types = {
            "primary": "primary_uuid",
            "non_primary": "non_primary_uuid",
            "no_salary": "no_salary_uuid",
            "fixed_primary": "fixed_primary_uuid",
        }

        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict({"status_code": 201, "text": lambda: "OK"})
        self.assertFalse(_mo_post.called)

        sd_updater._create_engagement_type = MagicMock()
        sd_updater._create_engagement_type.return_value = "new_engagement_type_uuid"

        sd_updater._create_professions = MagicMock()
        sd_updater._create_professions.return_value = "new_profession_uuid"

        sd_updater.update_all_employments()
        # We expect the exact following 4 calls to have been made
        self.assertEqual(len(_mo_post.mock_calls), 5)
        _mo_post.assert_has_calls(
            [
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "validity": {"from": "2020-11-10", "to": "2021-02-09"},
                            "primary": {"uuid": "non_primary_uuid"},
                        },
                    },
                ),
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "org_unit": {"uuid": "department_uuid"},
                            "validity": {"from": "2020-11-10", "to": None},
                        },
                    },
                ),
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "job_function": {"uuid": "new_profession_uuid"},
                            "validity": {"from": "2020-11-10", "to": None},
                        },
                    },
                ),
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "engagement_type": {"uuid": engagement_type},
                            "validity": {"from": "2020-11-10", "to": None},
                        },
                    },
                ),
                call(
                    "details/terminate",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "validity": {"from": "2021-02-09", "to": None},
                    },
                ),
            ]
        )
        sd_updater._create_engagement_type.assert_not_called()
        sd_updater._create_professions.assert_called_once()

    @given(from_date=st.dates(date(1970, 1, 1), date(2060, 1, 1)))
    @example(from_date=date.today())
    def test_date_tuples(self, from_date):
        def num_days_between(start, end):
            delta = end - start
            return delta.days

        today = date.today()
        if from_date >= today:
            # Cannot synchronize into the future
            num_expected_intervals = 0
        else:
            num_expected_intervals = num_days_between(from_date, today)

        # Construct datetime at from_date midnight
        from_datetime = datetime.combine(from_date, datetime.min.time())

        dates = list(gen_date_pairs(from_datetime))
        self.assertEqual(len(dates), num_expected_intervals)
        # We always expect intervals to be exactly one day long
        for from_date, to_date in dates:
            between = num_days_between(from_date, to_date)
            self.assertEqual(type(from_date), datetime)
            self.assertEqual(type(to_date), datetime)
            self.assertEqual(between, 1)

    @given(
        job_function=st.text(),
        exception=st.just(JobfunctionSettingsIsWrongException),
    )
    @example(job_function="JobPositionIdentifier", exception=None)
    @example(job_function="EmploymentName", exception=None)
    def test_job_function_configuration(self, job_function, exception):
        """Test that job_function only has two valid values."""
        if exception:
            with self.assertRaises(exception):
                setup_sd_changed_at({"integrations.SD_Lon.job_function": job_function})
        else:
            setup_sd_changed_at({"integrations.SD_Lon.job_function": job_function})

    @given(job_position=st.integers(), no_salary_minimum=st.integers())
    @patch("integrations.SD_Lon.sd_changed_at.sd_payloads", autospec=True)
    def test_construct_object(self, sd_payloads_mock, job_position, no_salary_minimum):
        expected = no_salary_minimum is not None
        expected = expected and job_position < no_salary_minimum
        expected = not expected

        sd_updater = setup_sd_changed_at(
            {
                "integrations.SD_Lon.no_salary_minimum_id": no_salary_minimum,
            }
        )
        sd_updater.apply_NY_logic = (
            lambda org_unit, user_key, validity, person_uuid: org_unit
        )

        morahelper = sd_updater.morahelper_mock
        morahelper.read_ou.return_value = {
            "org_unit_level": {
                "user_key": "IHaveNoIdea",
            },
            "uuid": "uuid-a",
        }
        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict({"status_code": 201, "text": lambda: "OK"})

        engagement = {
            "EmploymentIdentifier": "BIGAL",
            "EmploymentDepartment": [{"DepartmentUUIDIdentifier": "uuid-c"}],
            "Profession": [{"JobPositionIdentifier": str(job_position)}],
        }
        status = {
            "ActivationDate": "",
            "DeactivationDate": "",
            "EmploymentStatusCode": "",
        }
        cpr = ""
        result = sd_updater.create_new_engagement(engagement, status, cpr, "uuid-b")
        self.assertEqual(result, expected)
        if expected:
            sd_payloads_mock.create_engagement.assert_called_once()
        else:
            sd_payloads_mock.create_engagement.assert_not_called()

    @given(job_id=st.integers(min_value=0), engagement_exists=st.booleans())
    def test_fetch_engagement_type(self, job_id, engagement_exists):
        """Test that fetch_engagement_type only calls create when class is missing.

        This is done by creating the class if engagement_exists is set.
        I assume this works the same for _fetch_professions as they are similar.
        """
        sd_updater = setup_sd_changed_at()
        sd_updater.engagement_types = {
            "månedsløn": "monthly pay",
            "timeløn": "hourly pay",
        }

        self.assertEqual(len(sd_updater.engagement_types), 2)
        if engagement_exists:
            sd_updater.engagement_types[
                "engagement_type" + str(job_id)
            ] = "old_engagement_type_uuid"
            self.assertEqual(len(sd_updater.engagement_types), 3)
        else:
            self.assertEqual(len(sd_updater.engagement_types), 2)

        engagement_type_uuid = sd_updater._fetch_engagement_type(str(job_id))

        if engagement_exists:
            self.assertEqual(len(sd_updater.engagement_types), 3)
            sd_updater._create_class.assert_not_called()
            self.assertEqual(engagement_type_uuid, "old_engagement_type_uuid")
        else:
            self.assertEqual(len(sd_updater.engagement_types), 3)
            sd_updater._create_class.assert_called_once()
            sd_updater.job_sync.sync_from_sd.assert_called_once()
            self.assertIn("engagement_type" + str(job_id), sd_updater.engagement_types)
            self.assertEqual(engagement_type_uuid, "new_class_uuid")

    def test_edit_engagement(self):
        engagement = OrderedDict(
            [
                ("EmploymentIdentifier", "DEERE"),
                (
                    "Profession",
                    OrderedDict(
                        [
                            ("@changedAtDate", "1970-01-01"),
                            ("ActivationDate", "1960-01-01"),
                            ("DeactivationDate", "9999-12-31"),
                            ("JobPositionIdentifier", "9002"),
                            ("EmploymentName", "dummy"),
                            ("AppointmentCode", "0"),
                        ]
                    ),
                ),
            ]
        )

        sd_updater = setup_sd_changed_at()
        sd_updater.engagement_types = {
            "månedsløn": "monthly pay",
            "timeløn": "hourly pay",
        }
        sd_updater.mo_engagements_cache["person_uuid"] = [
            {
                "user_key": engagement["EmploymentIdentifier"],
                "uuid": "mo_engagement_uuid",
                "validity": {"from": "1950-01-01", "to": None},
            }
        ]

        morahelper = sd_updater.morahelper_mock
        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict(
            {
                "status_code": 201,
            }
        )
        sd_updater._create_engagement_type = MagicMock(
            wraps=sd_updater._create_engagement_type
        )
        sd_updater._create_professions = MagicMock(wraps=sd_updater._create_professions)
        # Return 1 on first call, 2 on second call
        sd_updater._create_class.side_effect = [
            "new_class_1_uuid",
            "new_class_2_uuid",
        ]

        sd_updater.edit_engagement(engagement, "person_uuid")

        # Check that the create functions are both called
        sd_updater._create_engagement_type.assert_called_with(
            "engagement_type9002", "9002"
        )
        sd_updater._create_professions.assert_called_with("9002", "9002")
        # And thus that job_sync is called once from each
        sd_updater.job_sync.sync_from_sd.assert_has_calls(
            [call("9002", refresh=True), call("9002", refresh=True)]
        )
        # And that the results are returned to MO
        _mo_post.assert_has_calls(
            [
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "job_function": {"uuid": "new_class_1_uuid"},
                            "validity": {"from": "1960-01-01", "to": None},
                        },
                    },
                ),
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "engagement_type": {"uuid": "new_class_2_uuid"},
                            "validity": {"from": "1960-01-01", "to": None},
                        },
                    },
                ),
            ]
        )

    @patch("integrations.SD_Lon.sd_common.sd_lookup_settings")
    @patch("integrations.SD_Lon.sd_changed_at.sd_lookup")
    def test_edit_engagement_job_position_id_set_to_value_above_9000(
        self, mock_sd_lookup, mock_sd_lookup_settings
    ):
        """
        If an employment exists in MO but with no engagement (e.g. which happens
        when the MO employment was created with an SD payload having a
        JobPositionIdentifier < 9000) and we receive an SD change payload, where
        the JobPositionIdentifier is set to a value greater than 9000, then we
        must ensure that an engagement is create for the corresponding employee
        in MO.
        """

        # Arrange

        sd_updater = setup_sd_changed_at(
            {
                "integrations.SD_Lon.monthly_hourly_divide": 80000,
                "integrations.SD_Lon.no_salary_minimum_id": 9000,
                "integrations.SD_Lon.import.too_deep": [
                    "Afdelings-niveau",
                    "NY1-niveau",
                ],
            }
        )

        sd_updater.engagement_types = {
            "månedsløn": "monthly pay",
            "timeløn": "hourly pay",
        }

        engagement = OrderedDict(
            [
                ("EmploymentIdentifier", "DEERE"),
                (
                    "Profession",
                    OrderedDict(
                        [
                            ("@changedAtDate", "2021-12-20"),
                            ("ActivationDate", "2021-12-19"),
                            ("DeactivationDate", "9999-12-31"),
                            ("JobPositionIdentifier", "9002"),
                            ("EmploymentName", "dummy"),
                            ("AppointmentCode", "0"),
                        ]
                    ),
                ),
            ]
        )
        mock_sd_lookup_settings.return_value = ("", "", "")

        # Necessary for the _find_engagement call in edit_engagement
        # Mock the call to arrange that no engagements are found for the user
        mora_helper = sd_updater.morahelper_mock
        _mo_lookup = mora_helper._mo_lookup
        _mo_lookup.return_value = []

        # Mock the call in sd_updater.read_employment_at(...)
        mock_sd_lookup.return_value = get_employment_fixture(
            1234561234, "emp_id", "dep_id", "dep_uuid", "9002", "job_title"
        )

        mock_apply_NY_logic = MagicMock()
        sd_updater.apply_NY_logic = mock_apply_NY_logic
        mock_apply_NY_logic.return_value = "org_unit_uuid"

        primary_types = sd_updater.primary_types_mock
        __getitem__ = primary_types.__getitem__
        __getitem__.return_value = "primary_uuid"

        _mo_post = mora_helper._mo_post
        _mo_post.return_value = attrdict(
            {
                "status_code": 201,
            }
        )

        # Act

        sd_updater.edit_engagement(engagement, "person_uuid")

        # Assert

        _mo_post.assert_called_once_with(
            "details/create",
            {
                "engagement_type": {"uuid": "new_class_uuid"},
                "fraction": 0,
                "job_function": {"uuid": "new_class_uuid"},
                "org_unit": {"uuid": "org_unit_uuid"},
                "person": {"uuid": "person_uuid"},
                "primary": {"uuid": "primary_uuid"},
                "type": "engagement",
                "user_key": "emp_id",
                "validity": {"from": "2020-11-10", "to": "2021-02-09"},
            },
        )

    #@parameterized.expand([("2021-10-15", "2021-10-14"), ("9999-12-31", None)])
    @patch("integrations.SD_Lon.sd_common.sd_lookup_settings")
    def test_edit_engagement_profession_job_position_id_set_to_value_below_9000(
        self, mock_sd_lookup_settings
    ):
        """
        If an employment exists in MO WITH an engagement and we receive an
        SD change payload, where the JobPositionIdentifier is set to a value
        less than 9000, then we must ensure that an engagement is terminated
        for the corresponding employee in MO.
        """

        # Arrange

        sd_updater = setup_sd_changed_at(
            {
                "integrations.SD_Lon.monthly_hourly_divide": 80000,
                "integrations.SD_Lon.no_salary_minimum_id": 9000,
                "integrations.SD_Lon.import.too_deep": [
                    "Afdelings-niveau",
                    "NY1-niveau",
                ],
            }
        )

        engagement = OrderedDict(
            [
                ("EmploymentIdentifier", "DEERE"),
                (
                    "Profession",
                    OrderedDict(
                        [
                            ("@changedAtDate", "2021-12-20"),
                            ("ActivationDate", "2021-12-19"),
                            ("DeactivationDate", "9999-12-31"),
                            ("JobPositionIdentifier", "8000"),
                            ("EmploymentName", "dummy"),
                            ("AppointmentCode", "0"),
                        ]
                    ),
                ),
            ]
        )

        mo_eng = {
            "user_key": "12345",
            "person": {
                "uuid": "person_uuid"
            }
        }

        mock_sd_lookup_settings.return_value = ("", "", "")

        # Mock the terminate engagement call
        mock_terminate_engagement = MagicMock()
        sd_updater._terminate_engagement = mock_terminate_engagement
        mock_terminate_engagement.return_value = True

        # Act

        sd_updater.edit_engagement_profession(engagement, mo_eng)

        # Assert

        mock_terminate_engagement.assert_called_once_with(
            "12345",
            "person_uuid",
            "2021-12-18",
            None
        )
