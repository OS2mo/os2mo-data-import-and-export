import pytest
from freezegun import freeze_time

from reports.RSD.Engagement_manager import extract_ancestors
from reports.RSD.Engagement_manager import get_age


@pytest.mark.parametrize(
    "cpr_number,expected_age",
    (("3011671296", 56), ("0112671296", 56), ("0212671296", 55)),
)
def test_get_age(cpr_number, expected_age):
    with freeze_time("2023-12-01"):
        assert get_age(cpr_number) == expected_age


@pytest.mark.parametrize(
    "ancestors,expected_list",
    (
        # Test top unit that has no ancestors
        ([], ["", "", "", "", "", "", ""]),
        # Test a unit with one ancestor
        ([{"name": "Niveau 1"}], ["Niveau 1", "", "", "", "", "", ""]),
        # Test 8 that with levels of ancestors we return the top 7.
        (
            [
                {"name": "Niveau 8"},
                {"name": "Niveau 7"},
                {"name": "Niveau 6"},
                {"name": "Niveau 5"},
                {"name": "Niveau 4"},
                {"name": "Niveau 3"},
                {"name": "Niveau 2"},
                {"name": "Niveau 1"},
            ],
            [
                "Niveau 1",
                "Niveau 2",
                "Niveau 3",
                "Niveau 4",
                "Niveau 5",
                "Niveau 6",
                "Niveau 7",
            ],
        ),
    ),
)
def test_extract_ancestors(ancestors, expected_list):
    assert extract_ancestors(ancestors) == expected_list
