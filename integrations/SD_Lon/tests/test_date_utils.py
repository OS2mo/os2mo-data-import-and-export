from collections import OrderedDict
from datetime import datetime

import pytest
from parameterized import parameterized

from integrations.SD_Lon.date_utils import _get_employment_from_date
from integrations.SD_Lon.date_utils import get_employment_dates
from integrations.SD_Lon.date_utils import sd_to_mo_termination_date


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
