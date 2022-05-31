import unittest
from unittest.mock import patch
from uuid import uuid4

from os2sync_export.os2mo import get_work_address
from os2sync_export.os2mo import is_ignored
from os2sync_export.os2mo import kle_to_orgunit
from os2sync_export.os2mo import overwrite_unit_uuids
from os2sync_export.os2mo import overwrite_user_uuids
from os2sync_export.os2mo import partition_kle
from parameterized import parameterized
from tests.helpers import dummy_settings
from tests.helpers import MockOs2moGet


class TestsMOAd(unittest.TestCase):
    def test_is_ignored(self):
        settings = dummy_settings
        il1, il2, iu1, iu2 = [uuid4() for u in range(4)]
        settings.os2sync_ignored_unit_levels = [il1, il2]
        settings.os2sync_ignored_unit_types = [iu1, iu2]
        unit = {
            "org_unit_level": {"uuid": str(uuid4())},
            "org_unit_type": {"uuid": str(uuid4())},
        }
        self.assertFalse(is_ignored(unit, settings))
        unit = {
            "org_unit_level": {"uuid": str(uuid4())},
            "org_unit_type": {"uuid": str(iu2)},
        }
        self.assertTrue(is_ignored(unit, settings))
        unit = {
            "org_unit_level": {"uuid": str(il1)},
            "org_unit_type": {"uuid": str(uuid4())},
        }
        self.assertTrue(is_ignored(unit, settings))

    @parameterized.expand(
        [
            ({"os2mo_has_kle": True}, ["2", "4", "6"], []),
            (
                {"os2mo_has_kle": True, "os2sync_use_contact_for_tasks": True},
                ["2", "4"],
                ["6"],
            ),
        ]
    )
    def test_kle_to_orgunit(
        self, testsettings, expected_tasks, expected_contactfortasks
    ):

        kles = [
            {
                "uuid": 1,
                "kle_aspect": [{"name": "Udførende"}],
                "kle_number": {"uuid": "2"},
            },
            {
                "uuid": 3,
                "kle_aspect": [{"name": "Udførende"}],
                "kle_number": {"uuid": "4"},
            },
            {
                "uuid": 5,
                "kle_aspect": [{"name": "Ansvarlig"}],
                "kle_number": {"uuid": "6"},
            },
        ]
        settings = dummy_settings
        settings.os2sync_use_contact_for_tasks = (
            True if testsettings.get("os2sync_use_contact_for_tasks") else False
        )

        tasks, contact_for_tasks = partition_kle(
            kles, settings.os2sync_use_contact_for_tasks
        )
        self.assertListEqual(expected_tasks, tasks)
        self.assertListEqual(expected_contactfortasks, contact_for_tasks)
        org_unit = {}
        kle_to_orgunit(
            org_unit, kles, use_contact_for_tasks=settings.os2sync_use_contact_for_tasks
        )
        self.assertListEqual(expected_tasks, org_unit.get("Tasks"))
        self.assertListEqual(
            expected_contactfortasks, org_unit.get("ContactForTasks", [])
        )

    @parameterized.expand(
        [
            (["Henvendelsessted", "Adresse"], "Henvendelsesstednavn"),
            (["Adresse", "Henvendelsessted"], "Adressenavn"),
            ([], None),
        ]
    )
    def test_get_work_address(self, work_address_names, expected):
        addresses = [
            {
                "name": "Henvendelsesstednavn",
                "address_type": {"name": "Henvendelsessted"},
            },
            {"name": "Adressenavn", "address_type": {"name": "Adresse"}},
            {"name": "Adressenavn2", "address_type": {"name": "Adresse"}},
        ]
        positions = [{"is_primary": True, "OrgUnitUuid": "Some_unit_uuid"}]
        with patch(
            "os2sync_export.os2mo.os2mo_get", return_value=MockOs2moGet(addresses)
        ):

            work_address = get_work_address(positions, work_address_names)
            self.assertEqual(work_address, expected)

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
            "os2sync_export.os2mo.os2mo_get",
            side_effect=[MockOs2moGet(it_system), MockOs2moGet(parent_it_system)],
        ):
            overwrite_unit_uuids(test_org, ["FK-org uuid", "AD ObjectGUID"])
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
            "os2sync_export.os2mo.os2mo_get",
            side_effect=[MockOs2moGet(it_system), MockOs2moGet(position_it_systems)],
        ):
            overwrite_user_uuids(test_user, ["FK-org uuid", "AD ObjectGUID"])
        assert test_user == expected
