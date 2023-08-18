from unittest.mock import MagicMock

import pytest

from exporters.os2rollekatalog.config import RollekatalogSettings
from exporters.os2rollekatalog.os2rollekatalog_integration import RollekatalogsExporter

MO_TEST_ENG_1 = {
    "job_function": {"name": "tester", "uuid": "job_function_uuid"},
    "org_unit": {"uuid": "org_unit_uuid"},
}
MO_TEST_ENG_2 = {
    "job_function": {"name": "QA", "uuid": "job_function_uuid2"},
    "org_unit": {"uuid": "org_unit_uuid2"},
}
POSITIONS_1 = {"name": "tester", "orgUnitUuid": "org_unit_uuid"}
POSITIONS_2 = {"name": "QA", "orgUnitUuid": "org_unit_uuid2"}
POSITIONS_TITLES_1 = {
    "name": "tester",
    "titleUuid": "job_function_uuid",
    "orgUnitUuid": "org_unit_uuid",
}
POSITIONS_TITLES_2 = {
    "name": "QA",
    "titleUuid": "job_function_uuid2",
    "orgUnitUuid": "org_unit_uuid2",
}


class MockRollekatalogExporter(RollekatalogsExporter):
    def _get_mora_helper(self, mora_base):
        return MagicMock()


@pytest.mark.parametrize(
    "current,future",
    [
        # No engagements in MO - No positions to rollekataloget
        ([], []),
        # One engagement
        ([MO_TEST_ENG_1], []),
        ([], [MO_TEST_ENG_1]),
        # More than one engagement:
        ([MO_TEST_ENG_1], [MO_TEST_ENG_2]),
        ([MO_TEST_ENG_1, MO_TEST_ENG_2], []),
        ([], [MO_TEST_ENG_1, MO_TEST_ENG_2]),
    ],
)
def test_get_employee_engagements(current, future):
    re = MockRollekatalogExporter(settings=RollekatalogSettings())
    re.mh._mo_lookup.side_effect = [current, future]
    positions = re.get_employee_engagements("dummy_uuid")
    assert list(positions) == current + future
    assert re.mh._mo_lookup.call_count == 2


@pytest.mark.parametrize(
    "sync_titles,engagement,expected",
    [
        # Without syncing titles:
        (False, MO_TEST_ENG_1, POSITIONS_1),
        (False, MO_TEST_ENG_2, POSITIONS_2),
        # Sync titles
        (True, MO_TEST_ENG_1, POSITIONS_TITLES_1),
        (True, MO_TEST_ENG_2, POSITIONS_TITLES_2),
    ],
)
def test_convert_position(sync_titles, engagement, expected):
    settings = RollekatalogSettings(exporters_os2rollekatalog_sync_titles=sync_titles)
    re = MockRollekatalogExporter(settings=settings)

    assert re.convert_position(engagement) == expected
