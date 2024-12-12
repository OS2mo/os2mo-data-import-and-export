import pytest
from freezegun import freeze_time

from reports.RSD.Engagement_manager import get_age


@pytest.mark.parametrize(
    "cpr_number,expected_age",
    (("3011671296", 56), ("0112671296", 56), ("0212671296", 55)),
)
def test_get_age(cpr_number, expected_age):
    with freeze_time("2023-12-01"):

        assert get_age(cpr_number) == expected_age
