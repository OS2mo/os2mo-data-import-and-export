from collections import OrderedDict
from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st
from more_itertools import pairwise
from parameterized import parameterized

from sdlon.date_utils import _get_employment_from_date
from sdlon.date_utils import date_to_datetime
from sdlon.date_utils import gen_cut_dates
from sdlon.date_utils import gen_date_intervals
from sdlon.date_utils import get_employment_dates
from sdlon.date_utils import is_midnight
from sdlon.date_utils import sd_to_mo_termination_date
from sdlon.date_utils import to_midnight


@given(st.dates())
def test_date_to_datetime(d: date) -> None:
    dt = date_to_datetime(d)
    assert isinstance(dt, datetime)
    assert d.year == dt.year
    assert d.month == dt.month
    assert d.day == dt.day
    assert 0 == dt.hour
    assert 0 == dt.minute
    assert 0 == dt.second
    assert 0 == dt.microsecond


@st.composite
def from_to_datetime(draw):
    """Generate date-intervals from 1930-->2060, where from < to."""
    from_datetimes = st.datetimes(
        min_value=datetime(1930, 1, 1),
        max_value=datetime(2050, 1, 1),
    )
    min_datetime = draw(from_datetimes)

    to_datetimes = st.datetimes(
        min_value=min_datetime + timedelta(seconds=1),
        max_value=datetime(2060, 1, 1),
    )
    max_datetime = draw(to_datetimes)

    return min_datetime, max_datetime


class TestSdToMoTerminationDate:
    def test_assert_string(self):
        with pytest.raises(AssertionError):
            sd_to_mo_termination_date(list())

    def test_assert_date_format_string(self):
        with pytest.raises(AssertionError):
            sd_to_mo_termination_date("invalid string")

    def test_assert_invalid_date(self):
        with pytest.raises(AssertionError):
            sd_to_mo_termination_date("2021-13-01")
        with pytest.raises(AssertionError):
            sd_to_mo_termination_date("2021-12-32")

    def test_subtract_one_day_from_sd_date(self):
        assert "2021-10-10" == sd_to_mo_termination_date("2021-10-11")
        assert "2021-02-07" == sd_to_mo_termination_date("2021-02-08")
        assert "2021-11-10" == sd_to_mo_termination_date("2021-11-11")
        assert "2021-10-25" == sd_to_mo_termination_date("2021-10-26")
        assert "2021-10-30" == sd_to_mo_termination_date("2021-10-31")
        assert "2021-01-29" == sd_to_mo_termination_date("2021-01-30")


@parameterized.expand([(False, datetime(2022, 2, 22)), (True, datetime(2011, 11, 11))])
def test_get_from_date(use_activation_date, date):
    employment = OrderedDict(
        [
            ("EmploymentDate", "2011-11-11"),
            ("EmploymentStatus", OrderedDict([("ActivationDate", "2022-02-22")])),
        ]
    )

    from_date = _get_employment_from_date(employment, use_activation_date)

    assert from_date == date


@parameterized.expand(
    [
        ("1960-01-01", "1970-01-01", datetime(1960, 1, 1)),
        ("1970-01-01", "1960-01-01", datetime(1960, 1, 1)),
        ("1970-01-01", "1970-01-01", datetime(1970, 1, 1)),
    ]
)
def test_get_employment_from_date_when_status_is_leave(
    emp_date,
    act_date,
    exp_date,
):
    employment = {
        "EmploymentDate": emp_date,
        "AnniversaryDate": "2004-08-15",
        "EmploymentStatus": {
            "EmploymentStatusCode": "3",
            "ActivationDate": act_date,
            "DeactivationDate": "9999-12-31",
        },
    }

    date_from, date_to = get_employment_dates(employment, False)

    assert date_from == exp_date


@parameterized.expand(
    [
        ("1960-01-01", datetime(1960, 1, 1)),
        ("1970-01-01", datetime(1970, 1, 1)),
    ]
)
def test_get_employment_to_date_when_status_is_leave(
    deactivation_date,
    exp_date,
):
    employment = {
        "EmploymentDate": "1970-01-01",
        "AnniversaryDate": "2004-08-15",
        "EmploymentStatus": {
            "EmploymentStatusCode": "3",
            "ActivationDate": "1975-01-01",
            "DeactivationDate": deactivation_date,
        },
    }

    date_from, date_to = get_employment_dates(employment, False)

    assert date_to == exp_date


@parameterized.expand(
    [
        [datetime(1960, 1, 1, 0, 0, 0, 0), datetime(1960, 1, 1, 0, 0, 0, 0)],
        [datetime(1960, 1, 1, 0, 0, 0, 1), datetime(1960, 1, 1, 0, 0, 0, 0)],
        [datetime(1960, 1, 1, 8, 0, 0, 0), datetime(1960, 1, 1, 0, 0, 0, 0)],
        [datetime(1960, 1, 1, 23, 59, 59, 999), datetime(1960, 1, 1, 0, 0, 0, 0)],
        [datetime(1960, 1, 2, 0, 0, 0, 0), datetime(1960, 1, 2, 0, 0, 0, 0)],
    ]
)
def test_to_midnight_parameterized(datetime, expected):
    assert to_midnight(datetime) == expected


@given(datetime=st.datetimes())
def test_to_midnight(datetime):
    midnight = to_midnight(datetime)
    assert midnight.date() == datetime.date()
    assert midnight.hour == 0
    assert midnight.minute == 0
    assert midnight.second == 0
    assert midnight.microsecond == 0


@parameterized.expand(
    [
        [datetime(1960, 1, 1, 0, 0, 0, 0), True],
        [datetime(1960, 1, 1, 0, 0, 0, 1), False],
        [datetime(1960, 1, 1, 8, 0, 0, 0), False],
        [datetime(1960, 1, 1, 23, 59, 59, 999), False],
        [datetime(1960, 1, 2, 0, 0, 0, 0), True],
    ]
)
def test_is_midnight(datetime, expected):
    assert is_midnight(datetime) is expected


@given(datetime=st.datetimes())
def test_to_midnight_is_midnight(datetime):
    assert is_midnight(to_midnight(datetime))


@parameterized.expand(
    [
        (
            datetime(1960, 1, 1, 8, 0, 0),
            datetime(1960, 1, 1, 9, 0, 0),
            [(datetime(1960, 1, 1, 8, 0, 0), datetime(1960, 1, 1, 9, 0, 0))],
        ),
        (
            datetime(1960, 1, 1, 8, 0, 0),
            datetime(1960, 1, 2, 9, 0, 0),
            [
                (datetime(1960, 1, 1, 8, 0, 0), datetime(1960, 1, 2, 0, 0, 0)),
                (datetime(1960, 1, 2, 0, 0, 0), datetime(1960, 1, 2, 9, 0, 0)),
            ],
        ),
        (
            datetime(1960, 1, 1, 8, 0, 0),
            datetime(1960, 1, 3, 9, 0, 0),
            [
                (datetime(1960, 1, 1, 8, 0, 0), datetime(1960, 1, 2, 0, 0, 0)),
                (datetime(1960, 1, 2, 0, 0, 0), datetime(1960, 1, 3, 0, 0, 0)),
                (datetime(1960, 1, 3, 0, 0, 0), datetime(1960, 1, 3, 9, 0, 0)),
            ],
        ),
    ]
)
def test_gen_date_intervals(from_date, to_date, expected):
    dates = gen_date_intervals(from_date, to_date)
    assert list(dates) == expected


def midnights_apart(from_datetime, to_datetime) -> int:
    """Return the number of day changes between from_datetime and to_datetime."""
    return (to_datetime.date() - from_datetime.date()).days


@given(datetimes=from_to_datetime())
def test_date_tuples(datetimes):
    from_datetime, to_datetime = datetimes

    dates = list(gen_cut_dates(from_datetime, to_datetime))
    assert dates[0] == from_datetime
    assert dates[-1] == to_datetime

    num_days_apart = midnights_apart(from_datetime, to_datetime)
    # Remove from_datetime and to_datetime from count, remove 1 if to is midnight
    assert len(dates) - 2 == num_days_apart - (1 if is_midnight(to_datetime) else 0)

    # We always expect intervals to be exactly one day long
    for from_datetime, to_datetime in pairwise(dates[1:-1]):
        num_days_apart = midnights_apart(from_datetime, to_datetime)
        assert type(from_datetime) == datetime
        assert type(to_datetime) == datetime
        assert num_days_apart == 1
        assert (to_datetime - from_datetime).total_seconds() == 86400
