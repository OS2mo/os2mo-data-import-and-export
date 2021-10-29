import pytest

from integrations.SD_Lon.convert import sd_to_mo_termination_date


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
