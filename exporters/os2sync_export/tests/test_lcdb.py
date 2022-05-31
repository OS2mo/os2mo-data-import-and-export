import unittest
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from os2sync_export.lcdb_os2mo import is_ignored
from os2sync_export.lcdb_os2mo import overwrite_unit_uuids
from os2sync_export.lcdb_os2mo import overwrite_user_uuids
from parameterized import parameterized
from tests.helpers import dummy_settings


def test_lcdb_is_ignored():
    ignored_level_uuid = uuid4()
    ignored_type_uuid = uuid4()
    used_level_uuid = uuid4()
    used_type_uuid = uuid4()
    settings = dummy_settings

    settings.os2sync_ignored_unit_levels = [ignored_level_uuid, uuid4()]
    settings.os2sync_ignored_unit_types = [ignored_type_uuid, uuid4()]

    # random uuids, not in settings
    unit = Mock(enhedsniveau_uuid=str(uuid4()), enhedstype_uuid=str(uuid4()))
    assert not is_ignored(unit, settings)
    # Unit type is ignored:
    unit.enhedstype_uuid = str(ignored_type_uuid)
    assert is_ignored(unit, settings)
    # Unit type is used:
    unit.enhedstype_uuid = str(used_type_uuid)
    assert not is_ignored(unit, settings)
    # ignored unit_level:
    unit.enhedsniveau_uuid = str(ignored_level_uuid)
    assert is_ignored(unit, settings)
    # Used unit_level:
    unit.enhedsniveau_uuid = str(used_level_uuid)
    assert not is_ignored(unit, settings)

    # No unit_level:
    unit.enhedsniveau_uuid = None
    assert not is_ignored(unit, settings)
    # No unit_type:
    unit.enhedstype_uuid = None
    assert not is_ignored(unit, settings)


class TestsLCDBMO(unittest.TestCase):
    @parameterized.expand(
        [
            # No (relevant) it systems - no change
            ([], [], {"Uuid": "old_uuid", "ParentOrgUnitUuid": "old_parent_uuid"}),
            (
                [{"itsystem": {"name": "irrelevant it system"}, "user_key": "dummy"}],
                [{"itsystem": {"name": "irrelevant it system"}, "user_key": "dummy"}],
                {"Uuid": "old_uuid", "ParentOrgUnitUuid": "old_parent_uuid"},
            ),
            # Overwrite both uuid and parent uuid
            (
                [{"itsystem": {"name": "FK-org uuid"}, "user_key": "fk-unit_uuid"}],
                [{"itsystem": {"name": "FK-org uuid"}, "user_key": "parent_uuid"}],
                {"Uuid": "fk-unit_uuid", "ParentOrgUnitUuid": "parent_uuid"},
            ),
            (
                [{"itsystem": {"name": "AD ObjectGUID"}, "user_key": "fk-unit_uuid"}],
                [{"itsystem": {"name": "AD ObjectGUID"}, "user_key": "parent_uuid"}],
                {"Uuid": "fk-unit_uuid", "ParentOrgUnitUuid": "parent_uuid"},
            ),
            # Two it-systems - use first from the given list (fk-org first, AD second)
            (
                [
                    {"itsystem": {"name": "FK-org uuid"}, "user_key": "right_uuid"},
                    {"itsystem": {"name": "AD ObjectGUID"}, "user_key": "wrong_uuid"},
                ],
                [],
                {"Uuid": "right_uuid", "ParentOrgUnitUuid": "old_parent_uuid"},
            ),
            (
                [],
                [
                    {
                        "itsystem": {"name": "FK-org uuid"},
                        "user_key": "right_parent_uuid",
                    },
                    {
                        "itsystem": {"name": "AD ObjectGUID"},
                        "user_key": "wrong_parent_uuid",
                    },
                ],
                {"Uuid": "old_uuid", "ParentOrgUnitUuid": "right_parent_uuid"},
            ),
        ]
    )
    def test_overwrite_unit_uuids(self, it_system, parent_it_system, expected):
        test_org = {"Uuid": "old_uuid", "ParentOrgUnitUuid": "old_parent_uuid"}
        with patch(
            "os2sync_export.lcdb_os2mo.lookup_unit_it_connections",
            side_effect=[it_system, parent_it_system],
        ):
            overwrite_unit_uuids(
                UnifiedAlchemyMagicMock(), test_org, ["FK-org uuid", "AD ObjectGUID"]
            )
        assert test_org == expected

    @parameterized.expand(
        [
            # No (relevant) it systems - no change
            (
                [],
                [],
                {"Uuid": "old_uuid", "Positions": [{"OrgUnitUuid": "old_unit_uuid"}]},
            ),
            # Person has a uuid in it-system
            (
                [
                    {
                        "itsystem": {"name": "AD ObjectGUID"},
                        "user_key": "new_uuid",
                    },
                ],
                [],
                {"Uuid": "new_uuid", "Positions": [{"OrgUnitUuid": "old_unit_uuid"}]},
            ),
            # Person has a position in a unit with a mapped uuid:
            (
                [],
                [
                    {
                        "itsystem": {"name": "AD ObjectGUID"},
                        "user_key": "new_uuid",
                    },
                ],
                {"Uuid": "old_uuid", "Positions": [{"OrgUnitUuid": "new_uuid"}]},
            ),
        ]
    )
    def test_overwrite_user_uuids(self, it_system, position_it_systems, expected):
        test_user = {
            "Uuid": "old_uuid",
            "Positions": [{"OrgUnitUuid": "old_unit_uuid"}],
        }
        with patch(
            "os2sync_export.lcdb_os2mo.lookup_unit_it_connections",
            side_effect=[position_it_systems],
        ):
            with patch(
                "os2sync_export.lcdb_os2mo.lookup_user_it_connections",
                side_effect=[it_system],
            ):
                overwrite_user_uuids(
                    UnifiedAlchemyMagicMock(),
                    test_user,
                    ["FK-org uuid", "AD ObjectGUID"],
                )
        assert test_user == expected
