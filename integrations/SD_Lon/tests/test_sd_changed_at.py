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
from integrations.SD_Lon.exceptions import JobfunctionSettingsIsWrongException
from integrations.SD_Lon.sd_changed_at import ChangeAtSD
from integrations.SD_Lon.sd_changed_at import gen_date_pairs
from test_case import DipexTestCase


class ChangeAtSDTest(ChangeAtSD):
    def __init__(self, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"

        self._get_job_sync = MagicMock()

        self._create_class = MagicMock()
        self._create_class.return_value = "new_class_uuid"

        super().__init__(*args, **kwargs)

    def _read_forced_uuids(self):
        return {}

    def _get_mora_helper(self, mora_base):
        return self.morahelper_mock


def setup_sd_changed_at(updates=None):
    settings = {
        "integrations.SD_Lon.job_function": "JobPositionIdentifier",
        "integrations.SD_Lon.use_ad_integration": False,
        "integrations.SD_Lon.monthly_hourly_divide": 8000,
        "mora.base": "dummy",
        "mox.base": "dummy",
    }
    if updates:
        settings.update(updates)

    today = date.today()
    start_date = today

    # TODO: Consider interfacing these off in seperate methods inside ChangeAtSD
    with patch("integrations.SD_Lon.sd_changed_at.primary_types", autospec=True):
        with patch(
            "integrations.SD_Lon.sd_changed_at.SDPrimaryEngagementUpdater",
            autospec=True,
        ):
            with patch(
                "integrations.SD_Lon.sd_changed_at.FixDepartments", autospec=True
            ):
                sd_updater = ChangeAtSDTest(
                    start_date, start_date + timedelta(days=1), settings
                )

    return sd_updater


def read_person_fixture(cpr, first_name, last_name, employment_id):
    institution_id = "XX"

    sd_request_reply = attrdict(
        {
            "text": """
        <GetPerson20111201 creationDateTime="2020-12-03T17:40:10">
            <RequestStructure>
                <InstitutionIdentifier>"""
            + institution_id
            + """</InstitutionIdentifier>
                <PersonCivilRegistrationIdentifier>"""
            + cpr
            + """</PersonCivilRegistrationIdentifier>
                <EffectiveDate>2020-12-03</EffectiveDate>
                <StatusActiveIndicator>true</StatusActiveIndicator>
                <StatusPassiveIndicator>false</StatusPassiveIndicator>
                <ContactInformationIndicator>false</ContactInformationIndicator>
                <PostalAddressIndicator>false</PostalAddressIndicator>
            </RequestStructure>
            <Person>
                <PersonCivilRegistrationIdentifier>"""
            + cpr
            + """</PersonCivilRegistrationIdentifier>
                <PersonGivenName>"""
            + first_name
            + """</PersonGivenName>
                <PersonSurnameName>"""
            + last_name
            + """</PersonSurnameName>
                <Employment>
                    <EmploymentIdentifier>"""
            + employment_id
            + """</EmploymentIdentifier>
                </Employment>
            </Person>
        </GetPerson20111201>
        """
        }
    )

    expected_read_person_result = [
        OrderedDict(
            [
                ("PersonCivilRegistrationIdentifier", cpr),
                ("PersonGivenName", first_name),
                ("PersonSurnameName", last_name),
                (
                    "Employment",
                    OrderedDict([("EmploymentIdentifier", employment_id)]),
                ),
            ]
        )
    ]

    return sd_request_reply, expected_read_person_result


def read_employment_fixture(cpr, employment_id, job_id, job_title, status="1"):
    institution_id = "institution_id"
    department_id = "deprtment_id"
    department_uuid = "department_uuid"

    sd_request_structure = (
        """
        <RequestStructure>
            <InstitutionIdentifier>"""
        + institution_id
        + """</InstitutionIdentifier>
            <ActivationDate>2020-11-01</ActivationDate>
            <ActivationTime>00:00:00</ActivationTime>
            <DeactivationDate>2020-12-02</DeactivationDate>
            <DeactivationTime>23:59:59</DeactivationTime>
            <DepartmentIndicator>true</DepartmentIndicator>
            <EmploymentStatusIndicator>true</EmploymentStatusIndicator>
            <ProfessionIndicator>true</ProfessionIndicator>
            <SalaryAgreementIndicator>false</SalaryAgreementIndicator>
            <SalaryCodeGroupIndicator>false</SalaryCodeGroupIndicator>
            <WorkingTimeIndicator>false</WorkingTimeIndicator>
            <UUIDIndicator>true</UUIDIndicator>
            <FutureInformationIndicator>false</FutureInformationIndicator>
        </RequestStructure>
    """
    )
    sd_request_person_employeed = (
        """
        <Person>
            <PersonCivilRegistrationIdentifier>"""
        + cpr
        + """</PersonCivilRegistrationIdentifier>
            <Employment>
                <EmploymentIdentifier>"""
        + employment_id
        + """</EmploymentIdentifier>
                <EmploymentDate>2020-11-10</EmploymentDate>
                <EmploymentDepartment changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <DepartmentIdentifier>"""
        + department_id
        + """</DepartmentIdentifier>
                    <DepartmentUUIDIdentifier>"""
        + department_uuid
        + """</DepartmentUUIDIdentifier>
                </EmploymentDepartment>
                <Profession changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <JobPositionIdentifier>"""
        + job_id
        + """</JobPositionIdentifier>
                    <EmploymentName>"""
        + job_title
        + """</EmploymentName>
                    <AppointmentCode>0</AppointmentCode>
                </Profession>
                <EmploymentStatus changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>2021-02-09</DeactivationDate>
                    <EmploymentStatusCode>1</EmploymentStatusCode>
                </EmploymentStatus>
                <EmploymentStatus changedAtDate="2020-11-10">
                    <ActivationDate>2021-02-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <EmploymentStatusCode>8</EmploymentStatusCode>
                </EmploymentStatus>
            </Employment>
        </Person>
    """
    )
    employeed_result = OrderedDict(
        [
            ("PersonCivilRegistrationIdentifier", cpr),
            (
                "Employment",
                OrderedDict(
                    [
                        ("EmploymentIdentifier", employment_id),
                        ("EmploymentDate", "2020-11-10"),
                        (
                            "EmploymentDepartment",
                            OrderedDict(
                                [
                                    ("@changedAtDate", "2020-11-10"),
                                    ("ActivationDate", "2020-11-10"),
                                    ("DeactivationDate", "9999-12-31"),
                                    ("DepartmentIdentifier", department_id),
                                    (
                                        "DepartmentUUIDIdentifier",
                                        department_uuid,
                                    ),
                                ]
                            ),
                        ),
                        (
                            "Profession",
                            OrderedDict(
                                [
                                    ("@changedAtDate", "2020-11-10"),
                                    ("ActivationDate", "2020-11-10"),
                                    ("DeactivationDate", "9999-12-31"),
                                    ("JobPositionIdentifier", job_id),
                                    ("EmploymentName", job_title),
                                    ("AppointmentCode", "0"),
                                ]
                            ),
                        ),
                        (
                            "EmploymentStatus",
                            [
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        ("ActivationDate", "2020-11-10"),
                                        ("DeactivationDate", "2021-02-09"),
                                        ("EmploymentStatusCode", "1"),
                                    ]
                                ),
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        ("ActivationDate", "2021-02-10"),
                                        ("DeactivationDate", "9999-12-31"),
                                        ("EmploymentStatusCode", "8"),
                                    ]
                                ),
                            ],
                        ),
                    ]
                ),
            ),
        ]
    )
    sd_request_person_deleted = (
        """
        <Person>
            <PersonCivilRegistrationIdentifier>"""
        + cpr
        + """</PersonCivilRegistrationIdentifier>
            <Employment>
                <EmploymentIdentifier>"""
        + employment_id
        + """</EmploymentIdentifier>
                <EmploymentStatus changedAtDate="2020-11-09">
                    <ActivationDate>2020-11-01</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <EmploymentStatusCode>S</EmploymentStatusCode>
                </EmploymentStatus>
            </Employment>
        </Person>
    """
    )
    deleted_result = OrderedDict(
        [
            ("PersonCivilRegistrationIdentifier", cpr),
            (
                "Employment",
                OrderedDict(
                    [
                        ("EmploymentIdentifier", employment_id),
                        (
                            "EmploymentStatus",
                            OrderedDict(
                                [
                                    ("@changedAtDate", "2020-11-09"),
                                    ("ActivationDate", "2020-11-01"),
                                    ("DeactivationDate", "9999-12-31"),
                                    ("EmploymentStatusCode", "S"),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )

    person_table = {
        "1": (sd_request_person_employeed, employeed_result),
        "S": (sd_request_person_deleted, deleted_result),
    }
    sd_response = (
        """
        <GetEmploymentChangedAtDate20111201 creationDateTime="2020-12-02T16:44:19">
        """
        + sd_request_structure
        + person_table[status][0]
        + """
        </GetEmploymentChangedAtDate20111201>
    """
    )
    sd_request_reply = attrdict({"text": sd_response})
    expected_read_employment_result = [person_table[status][1]]
    return sd_request_reply, expected_read_employment_result


class Test_sd_changed_at(DipexTestCase):
    @patch("integrations.SD_Lon.sd_common.sd_lookup_settings")
    @patch("integrations.SD_Lon.sd_common._sd_request")
    def test_read_person(self, sd_request, sd_settings):
        """Test that read_person does the expected transformation."""
        sd_settings.return_value = ("", "", "")

        cpr = "0101709999"
        sd_reply, expected_read_person_result = read_person_fixture(
            cpr=cpr, first_name="John", last_name="Deere", employment_id="01337"
        )

        sd_request.return_value = sd_reply

        sd_updater = setup_sd_changed_at()
        result = sd_updater.read_person(cpr=cpr)
        self.assertEqual(result, expected_read_person_result)

    def test_update_changed_persons(self):

        cpr = "0101709999"
        first_name = "John"
        last_name = "Deere"

        _, read_person_result = read_person_fixture(
            cpr=cpr,
            first_name=first_name,
            last_name=last_name,
            employment_id="01337",
        )

        sd_updater = setup_sd_changed_at()
        sd_updater.read_person = lambda cpr: read_person_result

        morahelper = sd_updater.morahelper_mock
        morahelper.read_organisation.return_value = "org_uuid"
        morahelper.read_user.return_value.__getitem__.return_value = "user_uuid"

        _mo_post = morahelper._mo_post
        self.assertFalse(_mo_post.called)
        sd_updater.update_changed_persons(cpr=cpr)
        _mo_post.assert_called_with(
            "e/create",
            {
                "givenname": first_name,
                "surname": last_name,
                "cpr_no": cpr,
                "org": {"uuid": "org_uuid"},
                "uuid": "user_uuid",
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
            sd_updater.create_new_engagement.assert_called_with(engagement, status, cpr)
        elif status == "S":  # Deletes call terminante engagement
            morahelper.read_user_engagement.return_value = [{"user_key": employment_id}]
            sd_updater._terminate_engagement = MagicMock()

            status = read_employment_result[0]["Employment"]["EmploymentStatus"]

            self.assertFalse(sd_updater._terminate_engagement.called)
            sd_updater.update_all_employments()
            sd_updater._terminate_engagement.assert_called_with(
                status["ActivationDate"], employment_id
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
        sd_updater.apply_NY_logic = lambda org_unit, user_key, validity: org_unit
        # Set globally shared state x(
        sd_updater.mo_person = {"uuid": "user_uuid"}
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

        sd_updater.create_new_engagement(engagement, status, cpr)
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

        _, read_employment_result = read_employment_fixture(
            cpr="0101709999",
            employment_id=employment_id,
            job_id="1234",
            job_title="EDB-Mand",
            status="S",
        )

        sd_updater = setup_sd_changed_at()

        morahelper = sd_updater.morahelper_mock

        # Set globally shared state x(
        sd_updater.mo_person = {"uuid": "user_uuid"}

        status = read_employment_result[0]["Employment"]["EmploymentStatus"]

        sd_updater.mo_engagement = [
            {
                "user_key": employment_id,
                "uuid": "mo_engagement_uuid",
            }
        ]

        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict({"status_code": 201, "text": lambda: "OK"})
        self.assertFalse(_mo_post.called)
        sd_updater._terminate_engagement(status["ActivationDate"], employment_id)
        _mo_post.assert_called_with(
            "details/terminate",
            {
                "type": "engagement",
                "uuid": "mo_engagement_uuid",
                "validity": {"to": "2020-10-31"},
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
        sd_updater.apply_NY_logic = lambda org_unit, user_key, validity: org_unit

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
                        "validity": {"to": "2021-02-09"},
                    },
                ),
            ]
        )
        sd_updater._create_engagement_type.assert_not_called()
        sd_updater._create_professions.assert_called_once()

    @given(
        from_date=st.dates(date(1970, 1, 1), date(2060, 1, 1)), one_day=st.booleans()
    )
    @example(from_date=date.today(), one_day=True)
    def test_date_tuples(self, from_date, one_day):
        def num_days_between(start, end):
            delta = end - start
            return delta.days

        today = date.today()
        if from_date >= today:
            # Cannot synchronize into the future
            num_expected_intervals = 0
        elif one_day:
            # one_day should always produce exactly one interval
            num_expected_intervals = 1
        else:
            num_expected_intervals = num_days_between(from_date, today)

        # Construct datetime at from_date midnight
        from_datetime = datetime.combine(from_date, datetime.min.time())

        dates = list(gen_date_pairs(from_datetime, one_day))
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
        sd_updater.apply_NY_logic = lambda org_unit, user_key, validity: org_unit

        morahelper = sd_updater.morahelper_mock
        morahelper.read_ou.return_value = {
            "org_unit_level": {
                "user_key": "IHaveNoIdea",
            },
            "uuid": "uuid-a",
        }
        _mo_post = morahelper._mo_post
        _mo_post.return_value = attrdict({"status_code": 201, "text": lambda: "OK"})

        sd_updater.mo_person = {
            "uuid": "uuid-b",
        }
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
        result = sd_updater.create_new_engagement(engagement, status, cpr)
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
        sd_updater.mo_engagement = [
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

        sd_updater.edit_engagement(engagement)

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
