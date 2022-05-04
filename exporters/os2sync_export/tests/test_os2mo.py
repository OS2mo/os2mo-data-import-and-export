import unittest
from unittest.mock import patch
from uuid import uuid4

from helpers import dummy_settings
from parameterized import parameterized

from integrations.os2sync.os2mo import get_work_address
from integrations.os2sync.os2mo import is_ignored
from integrations.os2sync.os2mo import kle_to_orgunit
from integrations.os2sync.os2mo import partition_kle
from integrations.os2sync.tests.helpers import MockOs2moGet


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
            "integrations.os2sync.os2mo.os2mo_get", return_value=MockOs2moGet(addresses)
        ):

            work_address = get_work_address(positions, work_address_names)
            self.assertEqual(work_address, expected)
