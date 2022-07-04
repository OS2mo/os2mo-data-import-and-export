from unittest.mock import MagicMock

import pytest

from exporters.os2rollekatalog.os2rollekatalog_integration import convert_position
from exporters.os2rollekatalog.os2rollekatalog_integration import (
    get_employee_engagements,
)

MO_TEST_ENG_1 = {
    "job_function": {"name": "tester", "uuid": "job_function_uuid"},
    "org_unit": {"uuid": "org_unit_uuid"},
}
POSITONS_1 = {"name": "tester", "orgUnitUuid": "org_unit_uuid"}
POSITONS_TITLES_1 = {
    "name": "tester",
    "titleUuid": "job_function_uuid",
    "orgUnitUuid": "org_unit_uuid",
}


@pytest.mark.parametrize(
    "current,future",
    [
        # No engagements in MO - No positions to rollekataloget
        ([], []),
        # One current engagement
        ([MO_TEST_ENG_1], []),
        # One future engagement
        ([], [MO_TEST_ENG_1]),
    ],
)
def test_get_employee_engagements(current, future):
    mh = MagicMock()
    mh._mo_lookup.side_effect = [current, future]
    positions = get_employee_engagements("dummy_uuid", mh)
    assert list(positions) == current + future
    assert mh._mo_lookup.call_count == 2


@pytest.mark.parametrize(
    "sync_titles,engagement,expected",
    [
        # One current engagement
        (False, MO_TEST_ENG_1, POSITONS_1),
        # One future engagement
        (True, MO_TEST_ENG_1, POSITONS_TITLES_1),
    ],
)
def test_convert_position(sync_titles, engagement, expected):
    assert convert_position(engagement, sync_titles=sync_titles) == expected
