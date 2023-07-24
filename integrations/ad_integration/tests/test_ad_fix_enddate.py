import datetime
import logging
from typing import Iterator
from unittest.mock import Mock
from unittest.mock import patch

import pytest as pytest
from click.testing import CliRunner
from hypothesis import given
from hypothesis import strategies as st

from ..ad_fix_enddate import ADEndDateSource
from ..ad_fix_enddate import ADUserEndDate
from ..ad_fix_enddate import cli
from ..ad_fix_enddate import CompareEndDate
from ..ad_fix_enddate import DEFAULT_TIMEZONE
from ..ad_fix_enddate import Invalid
from ..ad_fix_enddate import MOEngagementDateSource
from ..ad_fix_enddate import NegativeInfinity
from ..ad_fix_enddate import PositiveInfinity
from ..ad_fix_enddate import Unset
from ..ad_fix_enddate import UpdateEndDate
from ..ad_reader import ADParameterReader
from .mocks import AD_UUID_FIELD
from .mocks import MO_UUID
from .mocks import MockADParameterReader
from .mocks import MockADParameterReaderWithMOUUID


ENDDATE_FIELD = "enddate_field"
ENDDATE_FIELD_FUTURE = "enddate_field_future"
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


class _MockADEndDateSource(ADEndDateSource):
    def __init__(self):
        # Explicitly avoid calling `super().__init__(...)` as we don't want to create an
        # `ADParameterReader` instance.
        pass

    def __iter__(self) -> Iterator[ADUserEndDate]:
        raise NotImplementedError("must be implemented by subclass")


class _MockADEndDateSourceNoMatchingADUser(_MockADEndDateSource):
    def __iter__(self) -> Iterator[ADUserEndDate]:
        return iter([])


class _MockADEndDateSourceMatchingADUser(_MockADEndDateSource):
    def __iter__(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, ENDDATE_FIELD, None)


class _MockADEndDateSourceMatchingADUserAndEndDate(_MockADEndDateSource):
    def __iter__(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE)


class _MockADEndDateSourceMatchingADUserWrongEndDate(_MockADEndDateSource):
    def __iter__(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, ENDDATE_FIELD, "2023-01-01")


class _MockADEndDateSourceMatchingADUserWithSplitEndDates(_MockADEndDateSource):
    def __iter__(self) -> Iterator[ADUserEndDate]:
        yield ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE)
        yield ADUserEndDate(MO_UUID, ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE)


class _TestableCompareEndDate(CompareEndDate):
    def __init__(
        self,
        mo_engagement_date_source: MOEngagementDateSource,
        ad_end_date_source: ADEndDateSource,
        enddate_field_future: str | None = None,
    ):
        super().__init__(
            ENDDATE_FIELD,
            enddate_field_future,
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
    return datetime.datetime.fromisoformat(val).astimezone(DEFAULT_TIMEZONE)


def validity(start: str, end: str | None) -> dict:
    from_date = dt(start)
    to_date = dt(end) if end else None
    return {
        "validity": {
            "from": str(from_date),
            "to": str(to_date) if to_date else None,
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
        {
            "engagements": [
                {"objects": [{"validity": {"from": "2020-01-01", "to": VALID_AD_DATE}}]}
            ]
        }
    )


@pytest.fixture()
def mock_graphql_session_raising_keyerror():
    return _get_mock_graphql_session({})


@pytest.fixture()
def mock_mo_engagement_date_source(mock_graphql_session):
    return MOEngagementDateSource(mock_graphql_session)


@pytest.fixture()
def mock_mo_engagement_date_source_raising_keyerror(
    mock_graphql_session_raising_keyerror,
):
    return MOEngagementDateSource(mock_graphql_session_raising_keyerror)


@given(st.builds(Unset), st.builds(Unset))
def test_unset_class_always_equals_itself(unset_a: Unset, unset_b: Unset):
    assert unset_a == unset_b


@given(st.builds(Unset), st.datetimes() | st.none())
def test_unset_class_never_equals_other_types(unset: Unset, other: dict | None):
    assert unset != other


@given(st.builds(Unset))
def test_unset_repr_is_constant(unset: Unset):
    assert repr(unset) == "Unset()"


def test_mo_engagement_date_source_raises_keyerror_on_no_engagements():
    mock_mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session({"engagements": []})
    )
    with pytest.raises(KeyError):
        mock_mo_engagement_date_source.get_employee_engagement_dates(MO_UUID)


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
                            }
                        }
                    ]
                },
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2021-09-02T00:00:00+02:00",
                                "to": "2022-09-02T00:00:00+02:00",
                            }
                        }
                    ]
                },
                {
                    "objects": [
                        {
                            "validity": {
                                "from": "2022-09-02T00:00:00+02:00",
                                "to": "2023-09-02T00:00:00+02:00",
                            }
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
                            }
                        },
                        {
                            "validity": {
                                "from": "2021-09-02T00:00:00+02:00",
                                "to": "2022-09-02T00:00:00+02:00",
                            }
                        },
                        {
                            "validity": {
                                "from": "2022-09-02T00:00:00+02:00",
                                "to": "2023-09-02T00:00:00+02:00",
                            }
                        },
                    ]
                }
            ]
        },
    ],
)
def test_get_end_date(eng):
    mo_engagement_date_source = MOEngagementDateSource(_get_mock_graphql_session(eng))
    expected_latest_date = datetime.date(2023, 9, 2)
    actual_latest_date = mo_engagement_date_source.get_end_date(MO_UUID).date()
    assert actual_latest_date == expected_latest_date


def test_get_end_date_returns_unset_on_no_engagements():
    mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(engagement_objects())
    )
    assert mo_engagement_date_source.get_end_date(MO_UUID) == Unset()


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
                PositiveInfinity(),
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
                PositiveInfinity(),
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
                PositiveInfinity(),
                PositiveInfinity(),
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
                PositiveInfinity(),
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
                PositiveInfinity(),
            ),
        ),
    ],
)
def test_get_split_end_dates(mock_response, expected_split):
    mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(mock_response)
    )
    actual_split = mo_engagement_date_source.get_split_end_dates(MO_UUID)
    assert actual_split == expected_split


@pytest.mark.parametrize(
    "engagement_objects",
    [
        {"engagements": []},  # no engagements
        engagement_objects(),  # `engagements` key is present but `objects` is empty
    ],
)
def test_get_split_end_dates_returns_unset_tuple_on_no_engagements(engagement_objects):
    mock_mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(engagement_objects)
    )
    expected_result = Unset(), Unset()
    actual_result = mock_mo_engagement_date_source.get_split_end_dates(MO_UUID)
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "mock_engagements,mock_ad_enddate_source,expected_result",
    [
        # Case 1a: we are updating only the normal end date in AD, and the user only has
        # one engagement (in the past.) We should write the end date of that engagement
        # to the regular end date field in AD.
        (
            engagement_objects(validity(VALID_AD_DATE, VALID_AD_DATE)),
            _MockADEndDateSourceMatchingADUserAndEndDate(),
            [
                (
                    ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE),
                    dt(VALID_AD_DATE),
                ),
            ],
        ),
        # Case 1b: we are updating only the normal end date in AD, and the user only has
        # one engagement (in the past), whose end date is blank. We should write the end
        # date of that engagement to the regular end date field in AD.
        (
            engagement_objects(validity(VALID_AD_DATE, None)),
            _MockADEndDateSourceMatchingADUserAndEndDate(),
            [
                (
                    ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE),
                    PositiveInfinity(),
                ),
            ],
        ),
        # Case 2a: we are updating both the normal and the future end date in AD, and
        # the user only has one engagement (in the past.) We should write the end date
        # of that engagement to the regular end date field in AD, and leave the future
        # end date field in AD unset.
        (
            engagement_objects(validity(VALID_AD_DATE, VALID_AD_DATE)),
            _MockADEndDateSourceMatchingADUserWithSplitEndDates(),
            [
                (
                    ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE),
                    dt(VALID_AD_DATE),
                ),
                (
                    ADUserEndDate(MO_UUID, ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE),
                    Unset(),
                ),
            ],
        ),
        # Case 2b: we are updating both the normal and the future end date in AD, and
        # the user only has one engagement (in the past), whose end date is blank.  We
        # should write the end date of that engagement to the regular end date field in
        # AD, and leave the future end date field in AD unset.
        (
            engagement_objects(validity(VALID_AD_DATE, None)),
            _MockADEndDateSourceMatchingADUserWithSplitEndDates(),
            [
                (
                    ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE),
                    PositiveInfinity(),
                ),
                (
                    ADUserEndDate(MO_UUID, ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE),
                    Unset(),
                ),
            ],
        ),
    ],
)
def test_get_results(mock_engagements, mock_ad_enddate_source, expected_result):
    mock_mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(mock_engagements)
    )
    instance = _TestableCompareEndDate(
        mock_mo_engagement_date_source,
        mock_ad_enddate_source,
        enddate_field_future=ENDDATE_FIELD_FUTURE,
    )
    actual_result = list(instance.get_results())
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "input_mo_value,expected_mo_value",
    [
        # MO and AD values are identical, yield nothing
        (dt(VALID_AD_DATE), []),
        # MO and AD values differ, yield MO value
        (dt("2023-01-01"), dt("2023-01-01")),
        # MO value is None, yield "max date"
        (None, PositiveInfinity().as_datetime()),
        # MO value is positive infinity, yield its datetime value
        (PositiveInfinity(), PositiveInfinity().as_datetime()),
        # MO value is negative infinity, yield its datetime value
        (NegativeInfinity(), NegativeInfinity().as_datetime()),
        # MO value is unset, yield nothing
        (Unset(), []),
    ],
)
def test_get_changes_converts_symbolic_constants(input_mo_value, expected_mo_value):
    instance = _TestableCompareEndDate(
        None,  # mo_engagement_date_source, unused in this test
        None,  # ad_end_date_source, unused in this test
    )
    ad_user = ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE)
    # Patch the `get_results` method to return our mock data, so we can test how it is
    # processed by `get_changes`.
    with patch.object(
        instance,
        "get_results",
        return_value=((ad_user, mo_value) for mo_value in [input_mo_value]),
    ):
        changes = list(instance.get_changes())
        if expected_mo_value == []:
            assert len(changes) == 0
        else:
            assert len(changes) == 1
            assert changes[0] == (ad_user, expected_mo_value)


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
        (_MockADEndDateSourceMatchingADUser(), {MO_UUID: VALID_AD_DATE}),
        # If matching AD user exists *but* its AD end date is *not up to date*, return
        # the MO user UUID and MO end date.
        (_MockADEndDateSourceMatchingADUserWrongEndDate(), {MO_UUID: VALID_AD_DATE}),
    ],
)
def test_get_changes_single_end_date(
    mock_create_session,  # patch(...)
    mock_mo_engagement_date_source: MOEngagementDateSource,  # pytest fixture
    mock_ad_enddate_source: ADEndDateSource,  # pytest parametrize, arg 0
    expected_result,  # pytest parametrize, arg 1
):
    instance = _TestableCompareEndDate(
        mock_mo_engagement_date_source,
        mock_ad_enddate_source,
    )
    actual_result = {
        ad_user_end_date.mo_uuid: new_end_date.strftime("%Y-%m-%d")
        for ad_user_end_date, new_end_date in instance.get_changes()
    }
    assert actual_result == expected_result


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_get_changes_handles_keyerror(
    mock_create_session,
    mock_mo_engagement_date_source_raising_keyerror,
):
    instance = _TestableCompareEndDate(
        mock_mo_engagement_date_source_raising_keyerror,
        _MockADEndDateSourceMatchingADUser(),
    )
    assert list(instance.get_changes()) == []


@pytest.mark.parametrize(
    "reader,enddate_field_future,expected_result,expected_normalized_value",
    [
        (MockADParameterReader(), None, [], "<unused>"),
        (
            MockADParameterReaderWithMOUUID(),
            None,
            [ADUserEndDate(MO_UUID, ENDDATE_FIELD, Invalid())],
            Invalid(),
        ),
        (
            MockADParameterReaderWithMOUUIDAndInvalidDate(),
            None,
            [ADUserEndDate(MO_UUID, ENDDATE_FIELD, INVALID_AD_DATE)],
            Invalid(),
        ),
        (
            MockADParameterReaderWithMOUUIDAndValidSplitDates(),
            ENDDATE_FIELD_FUTURE,
            [
                ADUserEndDate(MO_UUID, ENDDATE_FIELD, VALID_AD_DATE),
                ADUserEndDate(MO_UUID, ENDDATE_FIELD_FUTURE, VALID_AD_DATE_FUTURE),
            ],
            Invalid(),
        ),
    ],
)
def test_ad_end_date_source(
    reader: ADParameterReader,
    enddate_field_future: str | None,
    expected_result: list[ADUserEndDate],
    expected_normalized_value: datetime.datetime | Invalid,
):
    def assert_result_matches(actual_result, expected_result):
        assert actual_result == expected_result
        if actual_result:
            assert [
                actual.normalized_value == expected_normalized_value
                for actual in actual_result
            ]

    with patch(
        "integrations.ad_integration.ad_fix_enddate.ADParameterReader",
        return_value=reader,
    ):
        instance = ADEndDateSource(
            AD_UUID_FIELD,
            ENDDATE_FIELD,
            enddate_field_future,
            settings=TEST_SETTINGS,
        )

        assert_result_matches(list(instance.of_all_users()), expected_result)
        assert_result_matches(list(instance.of_one_user("username")), expected_result)


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
@given(st.lists(st.tuples(st.builds(ADUserEndDate), st.datetimes()), max_size=10))
def test_run_all(mock_session, changes: list):
    u = _TestableUpdateEndDate()
    retval = u.run_all(changes, AD_UUID_FIELD)
    assert len(retval) == len(changes)
    for ps_script, ps_result in retval:
        assert "Set-ADUser" in ps_script
        assert ps_result == {}


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_run_all_logs_results_and_errors(mock_session, caplog):
    mock_changes = [
        (ADUserEndDate(MO_UUID, ENDDATE_FIELD, "2022-01-01"), dt("2022-06-01"))
    ]
    u = _TestableUpdateEndDateReturningError()
    with caplog.at_level(logging.DEBUG):
        u.run_all(mock_changes, AD_UUID_FIELD)
    assert len(caplog.records) == 5
    assert "Updating AD user" in caplog.records[0].message
    assert "Set-ADUser" in caplog.records[1].message  # command itself is logged
    assert "AD error response" in caplog.records[2].message  # error from AD is logged
    assert caplog.records[3].message == "0 users end dates corrected"
    assert caplog.records[4].message == "All end dates are fixed"


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(
    st.lists(
        st.tuples(st.builds(ADUserEndDate), st.datetimes()), min_size=1, max_size=1
    )
)
def test_run_all_supports_dry_run(mock_session, changes: list):
    u = _TestableUpdateEndDate()
    retval = u.run_all(changes, AD_UUID_FIELD, dry=True)
    assert len(retval) == len(changes)
    ps_script, ps_result = retval[0]
    assert "Set-ADUser" in ps_script
    assert ps_result == "<dry run>"
    assert len(u._ps_scripts_run) == 0


def mock_option_default(option, context, **kwargs):
    if option.name == "auth_server":
        return "http://keycloak"
    return None


@patch("click.core.Option.get_default", new=mock_option_default)
@patch("integrations.ad_integration.ad_common.read_settings", return_value={})
@patch("integrations.ad_integration.ad_common.AD._create_session")
@patch(
    "integrations.ad_integration.ad_fix_enddate.ADEndDateSource",
    cls=_MockADEndDateSourceNoMatchingADUser,
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
    mock_read_settings, mock_create_session, mock_ad_end_date_source, args, caplog
):
    runner = CliRunner()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(cli, args)
    if result.exception is not None:
        raise result.exception
    assert caplog.records[0].message.startswith("Command line args: ")
    assert caplog.records[-1].message == "All end dates are fixed"
