import unittest

from integrations.os2sync.os2mo import is_ignored, kle_to_orgunit


class TestsMOAd(unittest.TestCase):

    def test_is_ignored(self):
        settings = {"OS2SYNC_IGNORED_UNIT_LEVELS": ["10", "2"],
                    "OS2SYNC_IGNORED_UNIT_TYPES": ['6', '7']}
        unit = {"org_unit_level": {"uuid": "1"}, "org_unit_type": {"uuid": "5"}}
        self.assertFalse(is_ignored(unit, settings))
        unit = {"org_unit_level": {"uuid": "2"}, "org_unit_type": {"uuid": "5"}}
        self.assertTrue(is_ignored(unit, settings))
        unit = {"org_unit_level": {"uuid": "1"}, "org_unit_type": {"uuid": "6"}}
        self.assertTrue(is_ignored(unit, settings))

    def test_kle_to_orgunit(self):
        orgunit = {}
        kles = [
            {'uuid': 1, 'kle_number': {'uuid': '3'}},
            {'uuid': 2, 'kle_number': {'uuid': '4'}},
        ]
        kle_to_orgunit(orgunit, kles)
        self.assertEqual({"Tasks": ["3", "4"]}, orgunit)
