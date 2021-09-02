import unittest
from unittest.mock import patch

from parameterized import parameterized

from integrations.os2sync import config
from integrations.os2sync.os2mo import is_ignored
from integrations.os2sync.os2mo import kle_to_orgunit
from integrations.os2sync.os2mo import partition_kle


class TestsMOAd(unittest.TestCase):
    def test_is_ignored(self):
        settings = {
            "OS2SYNC_IGNORED_UNIT_LEVELS": ["10", "2"],
            "OS2SYNC_IGNORED_UNIT_TYPES": ["6", "7"],
        }
        unit = {"org_unit_level": {"uuid": "1"}, "org_unit_type": {"uuid": "5"}}
        self.assertFalse(is_ignored(unit, settings))
        unit = {"org_unit_level": {"uuid": "2"}, "org_unit_type": {"uuid": "5"}}
        self.assertTrue(is_ignored(unit, settings))
        unit = {"org_unit_level": {"uuid": "1"}, "org_unit_type": {"uuid": "6"}}
        self.assertTrue(is_ignored(unit, settings))

    @parameterized.expand(
        [
            ({"OS2MO_HAS_KLE": False}, ["2", "4", "6"], []),
            ({"OS2MO_HAS_KLE": True}, ["2", "4", "6"], []),
            ({"OS2MO_HAS_KLE": True, "use_contact_for_tasks": True}, ["2", "4"], ["6"]),
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
        with patch.dict(config.settings, testsettings):
            tasks, contact_for_tasks = partition_kle(kles)
            self.assertListEqual(expected_tasks, tasks)
            self.assertListEqual(expected_contactfortasks, contact_for_tasks)
            org_unit = {}
            org_unit = kle_to_orgunit(org_unit, kles)
            if testsettings.get("OS2MO_HAS_KLE"):
                self.assertListEqual(expected_tasks, org_unit.get("Tasks"))
                self.assertListEqual(
                    expected_contactfortasks, org_unit.get("ContactForTasks", [])
                )
            else:
                assert org_unit.get("Tasks") is None
                assert org_unit.get("ContactForTasks") is None
