import datetime
import logging
from typing import Iterator
from unittest.mock import Mock
from unittest.mock import patch

import pytest as pytest
from hypothesis import given
from hypothesis import HealthCheck
from hypothesis import settings
from hypothesis import strategies as st
from ramodels.mo import Validity

from ..ad_fix_enddate import ADEndDateSource
from ..ad_fix_enddate import ADUserEndDate
from ..ad_fix_enddate import CompareEndDate
from ..ad_fix_enddate import MOEngagementDateSource
from ..ad_fix_enddate import Unset
from ..ad_fix_enddate import UpdateEndDate
from ..ad_reader import ADParameterReader
from .mocks import AD_UUID_FIELD
from .mocks import MO_UUID
from .mocks import MockADParameterReader
from .mocks import MockADParameterReaderWithMOUUID


ENDDATE_FIELD = "enddate_field"
TEST_SEARCH_BASE = "search_base"
TEST_SETTINGS = {
    "primary": {
        "search_base": TEST_SEARCH_BASE,
        "system_user": "username",
        "password": "password",
    },
}


class _MockADEndDateSource(ADEndDateSource):
    def __init__(self):
        # Explicitly avoid calling `super().__init__(...)` as we don't want to create an
        # `ADParameterReader` instance.
        pass

    def get_all_matching_mo(self) -> Iterator[ADUserEndDate]:
        raise NotImplementedError("must be implemented by subclass")


class _MockADEndDateSourceNoMatchingADUser(_MockADEndDateSource):
    def get_all_matching_mo(self) -> Iterator[ADUserEndDate]:
        return iter([])


class _MockADEndDateSourceMatchingADUser(_MockADEndDateSource):
    def get_all_matching_mo(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, None)


class _MockADEndDateSourceMatchingADUserAndEndDate(_MockADEndDateSource):
    def get_all_matching_mo(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, "2022-12-31")


class _MockADEndDateSourceMatchingADUserWrongEndDate(_MockADEndDateSource):
    def get_all_matching_mo(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, "2023-01-01")


class _TestableCompareEndDate(CompareEndDate):
    def __init__(
        self,
        mo_engagement_date_source: MOEngagementDateSource,
        ad_end_date_source: ADEndDateSource,
    ):
        super().__init__(
            ENDDATE_FIELD,
            mo_engagement_date_source,
            ad_end_date_source,
        )


class _TestableUpdateEndDate(UpdateEndDate):
    def __init__(self):
        super().__init__(settings=TEST_SETTINGS)
        self._ps_scripts_run = []

    def _run_ps_script(self, ps_script):
        self._ps_scripts_run.append(ps_script)
        return {}


class _TestableUpdateEndDateReturningError(_TestableUpdateEndDate):
    def _run_ps_script(self, ps_script):
        super()._run_ps_script(ps_script)
        return {"error": "is mocked"}  # non-empty dict indicates a Powershell error


def dt(val: str):
    return datetime.datetime.fromisoformat(val).astimezone()


def validity(start, end) -> dict:
    obj = Validity(**{"from": start, "to": end})
    return {
        "validity": {
            "from": str(obj.from_date),
            "to": str(obj.to_date) if obj.to_date else None,
        }
    }


def engagement_objects(*validities: dict) -> dict:
    return {"engagements": [{"objects": validities}]}


def _get_mock_graphql_session(return_value):
    graphql_session = Mock()
    graphql_session.execute = Mock()
    graphql_session.execute.return_value = return_value
    return graphql_session


@pytest.fixture()
def mock_graphql_session():
    return _get_mock_graphql_session(
        {"engagements": [{"objects": [{"validity": {"to": "2022-12-31"}}]}]}
    )


@pytest.fixture()
def mock_graphql_session_raising_keyerror():
    return _get_mock_graphql_session({})


@pytest.fixture()
def mock_mo_engagement_date_source(mock_graphql_session):
    return MOEngagementDateSource(mock_graphql_session, 0)


@pytest.fixture()
def mock_mo_engagement_date_source_raising_keyerror(
    mock_graphql_session_raising_keyerror,
):
    return MOEngagementDateSource(mock_graphql_session_raising_keyerror, 0)


@given(st.builds(Unset), st.builds(Unset))
def test_unset_class_always_equals_itself(unset_a: Unset, unset_b: Unset):
    assert unset_a == unset_b


@given(st.builds(Unset), st.datetimes() | st.none())
def test_unset_class_never_equals_other_types(unset: Unset, other: dict | None):
    assert unset != other


@given(st.builds(Unset))
def test_unset_repr_is_constant(unset: Unset):
    assert repr(unset) == "Unset()"


@given(date=st.datetimes())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_to_enddate(date, mock_mo_engagement_date_source):
    assert mock_mo_engagement_date_source.to_enddate(str(date)) == date.date()
    assert (
        mock_mo_engagement_date_source.to_enddate(None)
        == MOEngagementDateSource._ad_null_date
    )
    assert (
        mock_mo_engagement_date_source.to_enddate("9999-12-31")
        == MOEngagementDateSource._ad_null_date
    )


@pytest.mark.parametrize(
    "eng",
    [
        {
            "engagements": [
                {"objects": [{"validity": {"to": "2021-09-02T00:00:00+02:00"}}]},
                {"objects": [{"validity": {"to": "2022-09-02T00:00:00+02:00"}}]},
                {"objects": [{"validity": {"to": "2023-09-02T00:00:00+02:00"}}]},
            ]
        },
        {
            "engagements": [
                {
                    "objects": [
                        {"validity": {"to": "2021-09-02T00:00:00+02:00"}},
                        {"validity": {"to": "2022-09-02T00:00:00+02:00"}},
                        {"validity": {"to": "2023-09-02T00:00:00+02:00"}},
                    ]
                }
            ]
        },
    ],
)
def test_get_employee_end_date(eng):
    mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(eng), 0
    )
    known_latest_date = datetime.date(2023, 9, 2)
    found_latest_date = mo_engagement_date_source.get_employee_end_date(MO_UUID)
    print(found_latest_date)
    assert found_latest_date == known_latest_date


@pytest.mark.parametrize(
    "mock_response,expected_split",
    [
        # Case A1: only past and present engagements; latest engagement has a blank end
        # date.
        (
            engagement_objects(
                validity("2020-01-01", "2021-12-31"),
                validity("2022-01-01", None),
            ),
            (
                None,
                Unset(),
            ),
        ),
        # Case A2: only past and present engagements; latest engagement has a non-blank
        # end date.
        (
            engagement_objects(
                validity("2020-01-01", "2021-12-31"),
                validity("2022-01-01", "2022-12-31"),
            ),
            (
                dt("2022-12-31"),
                Unset(),
            ),
        ),
        # Case B1: only future engagements; earliest engagement has a blank end date
        (
            engagement_objects(
                validity("2024-01-01", None),
                validity("2025-01-01", None),
            ),
            (
                Unset(),
                None,
            ),
        ),
        # Case B2: only future engagements; earliest engagement has an end date
        (
            engagement_objects(
                validity("2024-01-01", "2024-12-31"),
                validity("2025-01-01", None),
            ),
            (
                Unset(),
                dt("2024-12-31"),
            ),
        ),
        # Case C1: engagements in the past, present and future. Both the latest
        # engagement in the present, and the earliest engagement in the future, have a
        # blank end date.
        (
            engagement_objects(
                validity("2020-01-01", "2021-12-31"),
                validity("2022-01-01", None),
                validity("2024-01-01", None),
                validity("2025-01-01", None),
            ),
            (
                None,
                None,
            ),
        ),
        # Case C2: engagements in the past, present and future. The latest engagement in
        # the present has a blank end date, while the earliest engagement in the future
        # has a non-blank end date.
        (
            engagement_objects(
                validity("2020-01-01", "2021-12-31"),
                validity("2022-01-01", None),
                validity("2024-01-01", "2024-12-31"),
                validity("2025-01-01", None),
            ),
            (
                None,
                dt("2024-12-31"),
            ),
        ),
        # Case C3: engagements in the past, present and future. The latest engagement in
        # the present has a non-blank end date, while the earliest engagement in the
        # future has a blank end date.
        (
            engagement_objects(
                validity("2020-01-01", "2021-12-31"),
                validity("2022-01-01", "2022-12-31"),
                validity("2024-01-01", None),
                validity("2025-01-01", None),
            ),
            (
                dt("2022-12-31"),
                None,
            ),
        ),
    ],
)
def test_split_engagement_dates(mock_response, expected_split):
    mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(mock_response), 0
    )
    actual_split = mo_engagement_date_source.split_engagement_dates(MO_UUID)
    assert actual_split == expected_split


def test_split_engagement_dates_raises_exception_on_no_engagements():
    mock_mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(engagement_objects()), 0
    )
    with pytest.raises(Exception):
        mock_mo_engagement_date_source.split_engagement_dates(MO_UUID)


def test_get_employee_end_date_raises_keyerror_on_no_engagements():
    mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session({"engagements": []}), 0
    )
    with pytest.raises(KeyError):
        mo_engagement_date_source.get_employee_end_date(MO_UUID)


@patch("integrations.ad_integration.ad_common.AD._create_session")
@pytest.mark.parametrize(
    "mock_ad_enddate_source,expected_result",
    [
        # If no matching AD user, don't return a MO user UUID and MO end date
        (_MockADEndDateSourceNoMatchingADUser(), {}),
        # If matching AD user exists *and* its AD end date is already up to date, don't
        # return a MO user UUID and MO end date.
        (_MockADEndDateSourceMatchingADUserAndEndDate(), {}),
        # If matching AD user exists *but* its AD end date is *blank*, return the MO
        # user UUID and MO end date.
        (_MockADEndDateSourceMatchingADUser(), {MO_UUID: "2022-12-31"}),
        # If matching AD user exists *but* its AD end date is *not up to date*, return
        # the MO user UUID and MO end date.
        (_MockADEndDateSourceMatchingADUserWrongEndDate(), {MO_UUID: "2022-12-31"}),
    ],
)
def test_get_end_dates_to_fix(
    mock_create_session,
    mock_mo_engagement_date_source: MOEngagementDateSource,
    mock_ad_enddate_source: ADEndDateSource,
    expected_result,
):
    instance = _TestableCompareEndDate(
        mock_mo_engagement_date_source,
        mock_ad_enddate_source,
    )
    actual_result = instance.get_end_dates_to_fix(True)
    assert actual_result == expected_result


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_get_end_dates_to_fix_handles_keyerror(
    mock_create_session,
    mock_mo_engagement_date_source_raising_keyerror,
):
    instance = _TestableCompareEndDate(
        mock_mo_engagement_date_source_raising_keyerror,
        _MockADEndDateSourceMatchingADUser(),
    )
    assert instance.get_end_dates_to_fix(True) == {}


@pytest.mark.parametrize(
    "reader,expected_result",
    [
        (MockADParameterReader(), []),
        (MockADParameterReaderWithMOUUID(), [ADUserEndDate(MO_UUID, None)]),
    ],
)
def test_ad_end_date_source(
    reader: ADParameterReader, expected_result: list[ADUserEndDate]
):
    with patch(
        "integrations.ad_integration.ad_fix_enddate.ADParameterReader",
        return_value=reader,
    ):
        instance = ADEndDateSource(AD_UUID_FIELD, ENDDATE_FIELD, settings=TEST_SETTINGS)
        actual_result = list(instance.get_all_matching_mo())
        assert actual_result == expected_result


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(uuid=st.uuids(), enddate=st.dates())
def test_get_update_cmd(mock_session, uuid, enddate):
    u = _TestableUpdateEndDate()
    cmd = u.get_update_cmd(AD_UUID_FIELD, uuid, ENDDATE_FIELD, enddate)
    assert (
        cmd
        == f"""
        Get-ADUser  -SearchBase "{TEST_SEARCH_BASE}"  -Credential $usercredential -Filter \'{AD_UUID_FIELD} -eq "{uuid}"\' |
        Set-ADUser  -Credential $usercredential -Replace @{{{ENDDATE_FIELD}="{enddate}"}} |
        ConvertTo-Json
        """
    )


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(end_dates_to_fix=st.dictionaries(st.text(min_size=1), st.text() | st.none()))
def test_update_all(mock_session, end_dates_to_fix: dict):
    u = _TestableUpdateEndDate()
    retval = u.update_all(
        end_dates_to_fix,
        AD_UUID_FIELD,
        ENDDATE_FIELD,
        True,  # `print_commands`
        False,  # `dry_run`
    )
    assert len(retval) == len(end_dates_to_fix)
    for ps_script, ps_result in retval:
        assert "Set-ADUser" in ps_script
        assert ps_result == {}


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_update_all_logs_results_and_errors(mock_session, caplog):
    end_dates_to_fix = {"mock_uuid": "mock_end_date"}
    u = _TestableUpdateEndDateReturningError()
    with caplog.at_level(logging.INFO):
        u.update_all(
            end_dates_to_fix,
            AD_UUID_FIELD,
            ENDDATE_FIELD,
            True,  # `print_commands`
            False,  # `dry_run`
        )
    assert len(caplog.records) == 5
    assert caplog.records[0].message == "Command to run: "
    assert "Set-ADUser" in caplog.records[1].message  # command itself
    assert caplog.records[2].message.startswith("Result: ")  # command result
    assert caplog.records[3].message.endswith("users end dates corrected")
    assert caplog.records[4].message == "All end dates are fixed"


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_update_all_dry_run(mock_session):
    end_dates_to_fix = {"mock_uuid": "mock_end_date"}
    u = _TestableUpdateEndDate()
    retval = u.update_all(
        end_dates_to_fix,
        AD_UUID_FIELD,
        ENDDATE_FIELD,
        False,  # `print_commands`
        True,  # `dry_run`
    )
    assert len(retval) == len(end_dates_to_fix)
    ps_script, ps_result = retval[0]
    assert "Set-ADUser" in ps_script
    assert ps_result == "<dry run>"
    assert len(u._ps_scripts_run) == 0
