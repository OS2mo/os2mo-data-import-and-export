from datetime import date
from collections import OrderedDict
from unittest.mock import patch

from sdlon.config import CommonSettings
from sdlon.models import JobFunction
from sdlon.sd_common import read_employment_at


@patch("sdlon.sd_common.sd_lookup")
def test_return_none_when_sd_employment_empty(mock_sd_lookup):
    mock_sd_lookup.return_value = OrderedDict()
    settings = CommonSettings(
        sd_global_from_date=date(2000, 1, 1),
        sd_import_run_db="not used",
        sd_institution_identifier="dummy",
        sd_user="user",
        sd_password="password",
        sd_job_function=JobFunction.employment_name,
        sd_monthly_hourly_divide=1,
    )

    assert read_employment_at(date(2000, 1, 1), settings) is None
