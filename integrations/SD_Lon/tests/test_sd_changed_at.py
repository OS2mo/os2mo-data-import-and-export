import unittest
from collections import OrderedDict
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import hypothesis.strategies as st
import xmltodict
from hypothesis import given

from integrations.ad_integration.utils import AttrDict
from integrations.SD_Lon.sd_changed_at import ChangeAtSD


class ChangeAtSDTest(ChangeAtSD):
    def __init__(self, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"

        super().__init__(*args, **kwargs)

    def _get_mora_helper(self, mora_base):
        return self.morahelper_mock


def setup_sd_changed_at():
    settings = {
        "integrations.SD_Lon.job_function": "JobPositionIdentifier",
        "integrations.SD_Lon.use_ad_integration": False,
        "integrations.SD_Lon.monthly_hourly_divide": 8000,
        "mora.base": "dummy",
        "mox.base": "dummy",
    }

    today = date.today()
    start_date = today

    # TODO: Consider interfacing these off in seperate methods inside ChangeAtSD
    with patch(
        "integrations.SD_Lon.sd_changed_at.primary_types", autospec=True
    ) as pt:
        with patch(
            "integrations.SD_Lon.sd_changed_at.MOPrimaryEngagementUpdater",
            autospec=True,
        ) as pu:
            with patch(
                "integrations.SD_Lon.sd_changed_at.FixDepartments", autospec=True
            ) as fd:
                sd_updater = ChangeAtSDTest(
                    start_date, start_date + timedelta(days=1), settings
                )

    return sd_updater


def read_person_fixture(cpr, first_name, last_name, employment_id):
    institution_id = "XX"

    sd_request_reply = AttrDict(
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
    sd_request_reply = AttrDict({"text": sd_response})
    expected_read_employment_result = [person_table[status][1]]
    return sd_request_reply, expected_read_employment_result


class Test_sd_changed_at(unittest.TestCase):
    @patch("integrations.SD_Lon.sd_common._sd_request")
    def test_read_person(self, sd_request):
        """Test that read_person does the expected transformation."""

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
    @patch("integrations.SD_Lon.sd_common._sd_request")
    def test_read_employment_changed(self, sd_request, status):

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
                engagement, status, cpr
            )
        elif status == "S":  # Deletes call terminante engagement
            morahelper.read_user_engagement.return_value = [
                {"user_key": employment_id}
            ]
            sd_updater._terminate_engagement = MagicMock()

            status = read_employment_result[0]["Employment"]["EmploymentStatus"]

            self.assertFalse(sd_updater._terminate_engagement.called)
            sd_updater.update_all_employments()
            sd_updater._terminate_engagement.assert_called_with(
                status["ActivationDate"], employment_id
            )

    def test_create_new_engagement(self):

        cpr = "0101709999"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id="01337",
            job_id="1234",
            job_title="EDB-Mand",
        )

        sd_updater = setup_sd_changed_at()

        morahelper = sd_updater.morahelper_mock

        # Load noop NY logic
        sd_updater.apply_NY_logic = lambda org_unit, user_key, validity: org_unit
        sd_updater._add_profession_to_lora = lambda profession: {
            "uuid": "profession_uuid"
        }
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
        _mo_post.return_value = AttrDict(
            {
                "status_code": 201,
            }
        )
        self.assertFalse(_mo_post.called)
        sd_updater.create_new_engagement(engagement, status, cpr)
        _mo_post.assert_called_with(
            "details/create",
            {
                "type": "engagement",
                "org_unit": {"uuid": "department_uuid"},
                "person": {"uuid": "user_uuid"},
                "job_function": {"uuid": "profession_uuid"},
                "primary": {"uuid": "non_primary_uuid"},
                "engagement_type": {"uuid": None},
                "user_key": "01337",
                "fraction": 0,
                "validity": {"from": "2020-11-10", "to": "2021-02-09"},
            },
        )

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
        _mo_post.return_value = AttrDict({"status_code": 201, "text": lambda: "OK"})
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

    def test_update_all_employments_editing(self):

        cpr = "0101709999"
        employment_id = "01337"

        _, read_employment_result = read_employment_fixture(
            cpr=cpr,
            employment_id=employment_id,
            job_id="1234",
            job_title="EDB-Mand",
        )

        sd_updater = setup_sd_changed_at()
        sd_updater.read_employment_changed = lambda: read_employment_result
        # Load noop NY logic
        sd_updater.apply_NY_logic = lambda org_unit, user_key, validity: org_unit
        sd_updater._add_profession_to_lora = lambda profession: {
            "uuid": "profession_uuid"
        }

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
        _mo_post.return_value = AttrDict({"status_code": 201, "text": lambda: "OK"})
        from unittest.mock import call

        self.assertFalse(_mo_post.called)
        sd_updater.update_all_employments()
        # We expect the exact following 4 calls to have been made
        self.assertEqual(len(_mo_post.mock_calls), 4)
        _mo_post.assert_has_calls(
            [
                call(
                    "details/edit",
                    {
                        "type": "engagement",
                        "uuid": "mo_engagement_uuid",
                        "data": {
                            "validity": {"from": "2020-11-10", "to": "2021-02-09"},
                            "primary_type": {"uuid": "non_primary_uuid"},
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
                            "job_function": {"uuid": "profession_uuid"},
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
