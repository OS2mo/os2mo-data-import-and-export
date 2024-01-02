import datetime
import logging
from typing import Iterator
from unittest.mock import Mock
from unittest.mock import patch

import pytest as pytest
from click.testing import CliRunner
from freezegun import freeze_time
from hypothesis import given
from hypothesis import strategies as st

from ..ad_fix_enddate import _ParsedEngagement
from ..ad_fix_enddate import ADDate
from ..ad_fix_enddate import ADText
from ..ad_fix_enddate import ADUser
from ..ad_fix_enddate import ADUserSource
from ..ad_fix_enddate import ChangeList
from ..ad_fix_enddate import ChangeListExecutor
from ..ad_fix_enddate import cli
from ..ad_fix_enddate import DEFAULT_TIMEZONE
from ..ad_fix_enddate import Invalid
from ..ad_fix_enddate import MOEngagementSource
from ..ad_fix_enddate import MOSimpleEngagement
from ..ad_fix_enddate import MOSplitEngagement
from ..ad_fix_enddate import NegativeInfinity
from ..ad_fix_enddate import PositiveInfinity
from ..ad_fix_enddate import Unset
from ..ad_reader import ADParameterReader
from .mocks import AD_UUID_FIELD
from .mocks import MO_UUID
from .mocks import MockADParameterReader
from .mocks import MockADParameterReaderWithMOUUID


ENDDATE_FIELD = "enddate_field"
ENDDATE_FIELD_FUTURE = "enddate_field_future"
ORG_UNIT_PATH_FIELD_FUTURE = "orgunitpath_field_future"
TEST_SEARCH_BASE = "search_base"
TEST_SETTINGS = {
    "primary": {
        "search_base": TEST_SEARCH_BASE,
        "system_user": "username",
        "password": "password",
    },
}
VALID_AD_DATE = "2020-01-01"
VALID_AD_DATE_FUTURE = "2025-01-01"
INVALID_AD_DATE = "<invalid date>"
AD_ORG_UNIT_PATH_EMPTY = ADText(ORG_UNIT_PATH_FIELD_FUTURE, "")
AD_USER = ADUser(MO_UUID, None)  # type: ignore
AD_USER_VALID_DATE = ADUser(MO_UUID, ADDate(ENDDATE_FIELD, VALID_AD_DATE))
AD_USER_VALID_DATE_FUTURE = ADUser(
    MO_UUID,
    ADDate(ENDDATE_FIELD, VALID_AD_DATE),
    ADDate(ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE),
    None,  # start_date_future
    AD_ORG_UNIT_PATH_EMPTY,
)


class MockADParameterReaderWithMOUUIDAndInvalidDate(MockADParameterReaderWithMOUUID):
    def read_user(self, **kwargs):
        ad_user = super().read_user(**kwargs)
        ad_user[ENDDATE_FIELD] = INVALID_AD_DATE
        return ad_user


class MockADParameterReaderWithMOUUIDAndValidSplitDates(
    MockADParameterReaderWithMOUUID
):
    def read_user(self, **kwargs):
        ad_user = super().read_user(**kwargs)
        ad_user[ENDDATE_FIELD] = VALID_AD_DATE
        ad_user[ENDDATE_FIELD_FUTURE] = VALID_AD_DATE_FUTURE
        return ad_user


class _MockADUserSource(ADUserSource):
    def __init__(self):
        # Explicitly avoid calling `super().__init__(...)` as we don't want to create an
        # `ADParameterReader` instance.
        pass

    def __iter__(self) -> Iterator[ADUser]:
        raise NotImplementedError("must be implemented by subclass")


class _MockADUserSourceNoMatchingADUser(_MockADUserSource):
    def __iter__(self) -> Iterator[ADUser]:
        return iter([])


class _MockADUserSourceMatchingADUser(_MockADUserSource):
    def __iter__(self) -> Iterator[ADUser]:
        yield ADUser(MO_UUID, ADDate(ENDDATE_FIELD, None))


class _MockADUserSourceMatchingADUserAndEndDate(_MockADUserSource):
    def __iter__(self) -> Iterator[ADUser]:
        yield ADUser(MO_UUID, ADDate(ENDDATE_FIELD, VALID_AD_DATE))


class _MockADUserSourceMatchingADUserWrongEndDate(_MockADUserSource):
    def __iter__(self) -> Iterator[ADUser]:
        yield ADUser(MO_UUID, ADDate(ENDDATE_FIELD, "2023-01-01"))


class _MockADUserSourceMatchingADUserWithSplitEndDates(_MockADUserSource):
    def __iter__(self) -> Iterator[ADUser]:
        yield ADUser(
            MO_UUID,
            ADDate(ENDDATE_FIELD, VALID_AD_DATE),
            ADDate(ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE),
            None,  # start_date_future
            AD_ORG_UNIT_PATH_EMPTY,
        )


class _TestableChangeListExecutor(ChangeListExecutor):
    def __init__(self):
        super().__init__(settings=TEST_SETTINGS)
        self._ps_scripts_run = []

    def _run_ps_script(self, ps_script):
        self._ps_scripts_run.append(ps_script)
        return {}


class _TestableChangeListExecutorReturningError(_TestableChangeListExecutor):
    def _run_ps_script(self, ps_script):
        super()._run_ps_script(ps_script)
        return {"error": "is mocked"}  # non-empty dict indicates a Powershell error


def dt(val: str):
    parsed: datetime.datetime = datetime.datetime.fromisoformat(val)
    return parsed if parsed.tzinfo else parsed.astimezone(DEFAULT_TIMEZONE)


def validity(start: str, end: str | None) -> dict:
    from_date = dt(start)
    to_date = dt(end) if end else None
    return {
        "validity": {
            "from": str(from_date),
            "to": str(to_date) if to_date else None,
        }
    }


def org_unit() -> dict:
    return {
        "org_unit": [
            {
                "name": "Enhedsnavn",
                "ancestors_validity": [
                    {"name": "Forældreenhed"},
                    {"name": "Topenhed"},
                ],
            }
        ]
    }


def _expected_org_unit_path() -> str:
    return "Topenhed\\Forældreenhed\\Enhedsnavn"


def engagement_objects(*validities: dict) -> dict:
    objects: list[dict] = []
    for validity in validities:
        validity.update(org_unit())
        objects.append(validity)
    return {"engagements": [{"objects": objects}]}


def _get_mock_graphql_session(return_value):
    graphql_session = Mock()
    graphql_session.execute = Mock()
    graphql_session.execute.return_value = return_value
    return graphql_session


@pytest.fixture()
def mock_graphql_session():
    return _get_mock_graphql_session(
        {
            "engagements": [
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2020-01-01",
                                "to": VALID_AD_DATE,
                            },
                            **org_unit(),
                        }
                    ]
                }
            ]
        }
    )


@pytest.fixture()
def mock_graphql_session_raising_keyerror():
    return _get_mock_graphql_session({})


@pytest.fixture()
def mock_mo_engagement_source(mock_graphql_session):
    return MOEngagementSource(mock_graphql_session, split=False)


@pytest.fixture()
def mock_mo_engagement_source_raising_keyerror(
    mock_graphql_session_raising_keyerror,
):
    return MOEngagementSource(mock_graphql_session_raising_keyerror, split=False)


@given(st.builds(Unset), st.builds(Unset))
def test_unset_class_always_equals_itself(unset_a: Unset, unset_b: Unset):
    assert unset_a == unset_b


@given(st.builds(Unset), st.datetimes() | st.none())
def test_unset_class_never_equals_other_types(unset: Unset, other: dict | None):
    assert unset != other


@given(st.builds(Unset))
def test_unset_repr_is_constant(unset: Unset):
    assert repr(unset) == "Unset()"


def test_get_org_unit_path():
    parsed_engagement: _ParsedEngagement = _ParsedEngagement(
        from_dt=None,  # type: ignore
        to_dt=None,  # type: ignore
        org_unit=org_unit()["org_unit"],
    )
    assert parsed_engagement.get_org_unit_path() == _expected_org_unit_path()


def test_mo_engagement_source_raises_keyerror_on_no_engagements():
    mock_mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session({"engagements": []}),
        split=True,
    )
    with pytest.raises(KeyError):
        mock_mo_engagement_source._get_employee_engagements(MO_UUID)


@pytest.mark.parametrize(
    "eng",
    [
        {
            "engagements": [
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2020-09-02T00:00:00+02:00",
                                "to": "2021-09-02T00:00:00+02:00",
                            },
                            **org_unit(),
                        }
                    ]
                },
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2021-09-02T00:00:00+02:00",
                                "to": "2022-09-02T00:00:00+02:00",
                            },
                            **org_unit(),
                        }
                    ]
                },
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2022-09-02T00:00:00+02:00",
                                "to": "2023-09-02T00:00:00+02:00",
                            },
                            **org_unit(),
                        }
                    ]
                },
            ]
        },
        {
            "engagements": [
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2020-09-02T00:00:00+02:00",
                                "to": "2021-09-02T00:00:00+02:00",
                            },
                            **org_unit(),
                        },
                        {
                            "validity": {
                                "from": "2021-09-02T00:00:00+02:00",
                                "to": "2022-09-02T00:00:00+02:00",
                            },
                            **org_unit(),
                        },
                        {
                            "validity": {
                                "from": "2022-09-02T00:00:00+02:00",
                                "to": "2023-09-02T00:00:00+02:00",
                            },
                            **org_unit(),
                        },
                    ]
                }
            ]
        },
    ],
)
def test_get_simple_engagement(eng):
    mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session(eng),
        split=False,
    )
    expected_latest_date = datetime.date(2023, 9, 2)
    result: MOSimpleEngagement = mo_engagement_source.get_simple_engagement(AD_USER)
    actual_latest_date = result.end_date.date()
    assert actual_latest_date == expected_latest_date


def test_get_simple_engagement_returns_unset_on_no_engagements():
    mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session(engagement_objects()),
        split=False,
    )
    assert mo_engagement_source.get_simple_engagement(AD_USER) == MOSimpleEngagement(
        AD_USER, Unset()
    )


def test_date_conversion():
    mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session(
            engagement_objects(
                validity(
                    "2005-02-03T00:00:00+01:00",
                    "2038-09-09T00:00:00+02:00",
                )
            )
        ),
        split=False,
    )
    result: MOSplitEngagement = mo_engagement_source.get_split_engagement(AD_USER)
    assert result.end_date.date() == datetime.date(2038, 9, 9)


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
            MOSplitEngagement(
                AD_USER,
                PositiveInfinity(),
                Unset(),
                Unset(),
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
            MOSplitEngagement(
                AD_USER,
                dt("2022-12-31"),
                Unset(),
                Unset(),
                Unset(),
            ),
        ),
        # Case B1: only future engagements; earliest engagement has a blank end date
        (
            engagement_objects(
                validity("2024-01-01", None),
                validity("2025-01-01", None),
            ),
            MOSplitEngagement(
                AD_USER,
                Unset(),
                PositiveInfinity(),
                dt("2024-01-01"),
                _expected_org_unit_path(),
            ),
        ),
        # Case B2: only future engagements; earliest engagement has an end date
        (
            engagement_objects(
                validity("2024-01-01", "2024-12-31"),
                validity("2025-01-01", None),
            ),
            MOSplitEngagement(
                AD_USER,
                Unset(),
                dt("2024-12-31"),
                dt("2024-01-01"),
                _expected_org_unit_path(),
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
            MOSplitEngagement(
                AD_USER,
                PositiveInfinity(),
                PositiveInfinity(),
                dt("2024-01-01"),
                _expected_org_unit_path(),
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
            MOSplitEngagement(
                AD_USER,
                PositiveInfinity(),
                dt("2024-12-31"),
                dt("2024-01-01"),
                _expected_org_unit_path(),
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
            MOSplitEngagement(
                AD_USER,
                dt("2022-12-31"),
                PositiveInfinity(),
                dt("2024-01-01"),
                _expected_org_unit_path(),
            ),
        ),
    ],
)
def test_get_split_engagement(mock_response, expected_split):
    mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session(mock_response),
        split=True,
    )
    with freeze_time("2023-12-01"):
        actual_split: MOSplitEngagement = mo_engagement_source.get_split_engagement(
            AD_USER
        )
    assert actual_split == expected_split


@pytest.mark.parametrize(
    "engagement_objects",
    [
        {"engagements": []},  # no engagements
        engagement_objects(),  # `engagements` key is present but `objects` is empty
    ],
)
def test_get_split_engagement_returns_unset_values_on_no_engagements(
    engagement_objects,
):
    mock_mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session(engagement_objects),
        split=True,
    )
    expected_result = MOSplitEngagement(AD_USER, Unset(), Unset(), Unset(), Unset())
    actual_result = mock_mo_engagement_source.get_split_engagement(AD_USER)
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "enddate_field_future,mock_engagements,mock_ad_user_source,expected_result",
    [
        # Case 1a: we are updating only the normal end date in AD, and the user only has
        # one engagement (in the past.) We should write the end date of that engagement
        # to the regular end date field in AD.
        (
            None,
            engagement_objects(validity(VALID_AD_DATE, VALID_AD_DATE)),
            _MockADUserSourceMatchingADUserAndEndDate(),
            [],  # no changes emitted, as MO and AD end dates are already the same
        ),
        # Case 1b: we are updating only the normal end date in AD, and the user only has
        # one engagement (in the past), whose end date is blank. We should write the end
        # date of that engagement to the regular end date field in AD.
        (
            None,
            engagement_objects(validity(VALID_AD_DATE, None)),
            _MockADUserSourceMatchingADUserAndEndDate(),
            [MOSimpleEngagement(AD_USER_VALID_DATE, PositiveInfinity())],
        ),
        # Case 2a: we are updating both the normal and the future end date in AD, and
        # the user only has one engagement (in the past.) We should write the end date
        # of that engagement to the regular end date field in AD, and leave the future
        # end date field in AD unset.
        (
            ENDDATE_FIELD_FUTURE,
            engagement_objects(validity(VALID_AD_DATE, VALID_AD_DATE)),
            _MockADUserSourceMatchingADUserWithSplitEndDates(),
            [],  # no changes emitted, as MO and AD end dates are already the same
        ),
        # Case 2b: we are updating both the normal and the future end date in AD, and
        # the user only has one engagement (in the past), whose end date is blank.  We
        # should write the end date of that engagement to the regular end date field in
        # AD, and leave the future end date field in AD unset.
        (
            ENDDATE_FIELD_FUTURE,
            engagement_objects(validity(VALID_AD_DATE, None)),
            _MockADUserSourceMatchingADUserWithSplitEndDates(),
            [
                MOSplitEngagement(
                    AD_USER_VALID_DATE_FUTURE,
                    PositiveInfinity(),
                    Unset(),
                    Unset(),
                    Unset(),
                ),
            ],
        ),
    ],
)
def test_get_changes(
    enddate_field_future, mock_engagements, mock_ad_user_source, expected_result
):
    mock_mo_engagement_source = MOEngagementSource(
        _get_mock_graphql_session(mock_engagements),
        split=enddate_field_future is not None,
    )
    instance = ChangeList(mock_mo_engagement_source, mock_ad_user_source)
    actual_result = list(instance.get_changes())
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "input_mo_value,expected_return_value",
    [
        # MO and AD values are identical, yield nothing
        (dt(VALID_AD_DATE), None),
        # MO and AD values differ, yield MO value
        (dt("2023-01-01"), dt("2023-01-01")),
        # MO value is None, yield "max date"
        (None, PositiveInfinity().as_datetime()),
        # MO value is positive infinity, yield its datetime value
        (PositiveInfinity(), PositiveInfinity().as_datetime()),
        # MO value is negative infinity, yield its datetime value
        (NegativeInfinity(), NegativeInfinity().as_datetime()),
        # MO value is unset, yield nothing
        (Unset(), None),
    ],
)
def test_compare_end_date(input_mo_value, expected_return_value):
    ad_user = AD_USER_VALID_DATE
    instance = MOSimpleEngagement(ad_user, end_date=input_mo_value)
    actual_return_value = instance._compare_end_date(ad_user.end_date, input_mo_value)
    assert actual_return_value == expected_return_value


@patch("integrations.ad_integration.ad_common.AD._create_session")
@pytest.mark.parametrize(
    "mock_ad_user_source,expected_changes",
    [
        # If no matching AD user, don't return a MO user UUID and MO end date
        (_MockADUserSourceNoMatchingADUser(), None),
        # If matching AD user exists *and* its AD end date is already up to date, don't
        # return a MO user UUID and MO end date.
        (_MockADUserSourceMatchingADUserAndEndDate(), None),
        # If matching AD user exists *but* its AD end date is *blank*, return the MO
        # user UUID and MO end date.
        (_MockADUserSourceMatchingADUser(), {ENDDATE_FIELD: VALID_AD_DATE}),
        # If matching AD user exists *but* its AD end date is *not up to date*, return
        # the MO user UUID and MO end date.
        (
            _MockADUserSourceMatchingADUserWrongEndDate(),
            {ENDDATE_FIELD: VALID_AD_DATE},
        ),
    ],
)
def test_get_changes_simple_engagement(
    mock_create_session,  # patch(...)
    mock_mo_engagement_source,  # pytest fixture
    mock_ad_user_source: ADUserSource,  # pytest parametrize, arg 0
    expected_changes,  # pytest parametrize, arg 1
):
    instance = ChangeList(mock_mo_engagement_source, mock_ad_user_source)
    actual_result = list(instance.get_changes())
    if expected_changes:
        assert len(actual_result) == 1
        assert isinstance(actual_result[0], MOSimpleEngagement)
        assert actual_result[0].ad_user.mo_uuid == MO_UUID
        assert actual_result[0].changes == expected_changes
    else:
        assert len(actual_result) == 0


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_get_changes_handles_keyerror(
    mock_create_session,
    mock_mo_engagement_source_raising_keyerror,
):
    instance = ChangeList(
        mock_mo_engagement_source_raising_keyerror,
        _MockADUserSourceMatchingADUser(),
    )
    assert list(instance.get_changes()) == []


@pytest.mark.parametrize(
    "reader,enddate_field_future,expected_result,expected_normalized_value",
    [
        (MockADParameterReader(), None, [], "<unused>"),
        (
            MockADParameterReaderWithMOUUID(),
            None,
            [
                ADUser(
                    MO_UUID,
                    ADDate(ENDDATE_FIELD, Invalid()),
                    None,
                    None,
                    ADText(ORG_UNIT_PATH_FIELD_FUTURE, Invalid()),
                )
            ],
            Invalid(),
        ),
        (
            MockADParameterReaderWithMOUUIDAndInvalidDate(),
            None,
            [
                ADUser(
                    MO_UUID,
                    ADDate(ENDDATE_FIELD, INVALID_AD_DATE),
                    None,
                    None,
                    ADText(ORG_UNIT_PATH_FIELD_FUTURE, Invalid()),
                )
            ],
            Invalid(),
        ),
        (
            MockADParameterReaderWithMOUUIDAndValidSplitDates(),
            ENDDATE_FIELD_FUTURE,
            [
                ADUser(
                    MO_UUID,
                    ADDate(ENDDATE_FIELD, VALID_AD_DATE),
                    ADDate(ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE),
                    None,
                    ADText(ORG_UNIT_PATH_FIELD_FUTURE, Invalid()),
                ),
            ],
            Invalid(),
        ),
    ],
)
def test_ad_user_source(
    reader: ADParameterReader,
    enddate_field_future: str | None,
    expected_result: list[ADUser],
    expected_normalized_value: datetime.datetime | Invalid,
):
    def assert_matches(actual_list: list[ADUser], expected_list: list[ADUser]):
        actual: ADUser
        expected: ADUser
        for actual, expected in zip(actual_list, expected_list):
            assert actual == expected
            assert actual.end_date == expected.end_date
            assert actual.end_date_future == expected.end_date_future
            assert actual.start_date_future == expected.start_date_future
            assert actual.org_unit_path == expected.org_unit_path

    with patch(
        "integrations.ad_integration.ad_fix_enddate.ADParameterReader",
        return_value=reader,
    ):
        instance = ADUserSource(
            AD_UUID_FIELD,
            ENDDATE_FIELD,
            enddate_field_future,
            None,  # startdate_field_future
            ORG_UNIT_PATH_FIELD_FUTURE,
            settings=TEST_SETTINGS,
        )

        assert_matches(list(instance.of_all_users()), expected_result)
        assert_matches(list(instance.of_one_user("username")), expected_result)


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(uuid=st.uuids(), enddate=st.dates())
def test_get_update_cmd(mock_session, uuid, enddate):
    instance = _TestableChangeListExecutor()
    expected_cmd = f"""
    Get-ADUser -SearchBase "{TEST_SEARCH_BASE}" -Credential $usercredential -Filter \'{AD_UUID_FIELD} -eq "{uuid}"\' |
    Set-ADUser -Credential $usercredential -Replace @{{"{ENDDATE_FIELD}"="{enddate}"}} |
    ConvertTo-Json
    """
    actual_cmd = instance.get_update_cmd(
        AD_UUID_FIELD, uuid, **{ENDDATE_FIELD: enddate}
    )
    assert actual_cmd == instance.remove_redundant(expected_cmd)


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(
    st.lists(
        st.builds(MOSimpleEngagement) | st.builds(MOSplitEngagement),
        max_size=10,
    )
)
def test_run_all(mock_session, changes: list):
    instance = _TestableChangeListExecutor()
    retval = instance.run_all(changes, AD_UUID_FIELD)
    for ps_script, ps_result in retval:
        assert "Set-ADUser" in ps_script
        assert ps_result == {}


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_run_all_logs_results_and_errors(mock_session, caplog):
    mock_changes = [MOSimpleEngagement(AD_USER_VALID_DATE, dt("2022-01-01"))]
    instance = _TestableChangeListExecutorReturningError()
    with caplog.at_level(logging.DEBUG):
        instance.run_all(mock_changes, AD_UUID_FIELD)
    assert len(caplog.records) == 5
    assert "Updating AD user" in caplog.records[0].message
    assert "Set-ADUser" in caplog.records[1].message  # command itself is logged
    assert "AD error response" in caplog.records[2].message  # error from AD is logged
    assert caplog.records[3].message == "0 users end dates corrected"
    assert caplog.records[4].message == "All end dates are fixed"


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(
    st.lists(
        st.builds(MOSimpleEngagement) | st.builds(MOSplitEngagement),
        min_size=1,
        max_size=1,
    )
)
def test_run_all_supports_dry_run(mock_session, changes: list):
    instance = _TestableChangeListExecutor()
    retval = instance.run_all(changes, AD_UUID_FIELD, dry=True)
    if retval:
        ps_script, ps_result = retval[0]
        assert "Set-ADUser" in ps_script
        assert ps_result == "<dry run>"
    assert len(instance._ps_scripts_run) == 0


def mock_option_default(option, context, **kwargs):
    if option.name == "auth_server":
        return "http://keycloak"
    return None


@patch("click.core.Option.get_default", new=mock_option_default)
@patch("integrations.ad_integration.ad_common.read_settings", return_value={})
@patch("integrations.ad_integration.ad_common.AD._create_session")
@patch(
    "integrations.ad_integration.ad_fix_enddate.ADUserSource",
    cls=_MockADUserSourceNoMatchingADUser,
)
@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--ad-user", "foobar"],
        ["--dry-run"],
    ],
)
def test_cli(
    mock_read_settings,
    mock_create_session,
    mock_ad_user_source,
    args,
    caplog,
):
    runner = CliRunner()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(cli, args)
    if result.exception is not None:
        raise result.exception
    assert caplog.records[0].message.startswith("Command line args: ")
    assert caplog.records[-1].message == "All end dates are fixed"
