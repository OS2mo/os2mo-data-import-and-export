import unittest
import pathlib
import pprint
from exporters.sql_export.lora_cache import LoraCache
from tools.priority_by_class import choose_public_address, lc_choose_public_address

class LC:
    classes = {
        'aad0b1a0-e658-0aac-0a52-368bc5ec5b80': {
            'scope': 'PUBLIC',
        },
        'fa865555-58b5-327d-e7dc-2990b0d28ff9':{
            'scope': 'EMAIL',
        },
        '9ff8b38d-9f43-a74f-f28b-ebf5761d2e3a': {
            'scope': 'SECRET',
        }
    }

    addresses = {
        '1ff12bd9-7a5a-4837-9137-cb126c53f6ea': [
            {
                'uuid': '1ff12bd9-7a5a-4837-9137-cb126c53f6ea',
                'adresse_type': 'ra865555-58b5-327d-e7dc-2990b0d28ff9',
                'value': 'emailunknown@example.com',
                'visibility': None
            }
        ],
        '2ff12bd9-7a5a-4837-9137-cb126c53f6ea': [
            {
                'uuid': '2ff12bd9-7a5a-4837-9137-cb126c53f6ea',
                'adresse_type': 'fa865555-58b5-327d-e7dc-2990b0d28ff9',
                'value': 'emailsecret@example.com',
                'visibility': "9ff8b38d-9f43-a74f-f28b-ebf5761d2e3a"
            }
        ],
        '3ff12bd9-7a5a-4837-9137-cb126c53f6ea': [
            {
                'uuid': '3ff12bd9-7a5a-4837-9137-cb126c53f6ea',
                'adresse_type': 'fa865555-58b5-327d-e7dc-2990b0d28ff9',
                'value': 'emailpublic@example.com',
                'visibility': "aad0b1a0-e658-0aac-0a52-368bc5ec5b80"
            }
        ],
    }


class Tests(unittest.TestCase):

    @classmethod
    def OLDsetUpClass(self):
        self.lc = LoraCache(resolve_dar=False)
        try:
            self.lc.populate_cache(dry_run=True)
        except FileNotFoundError:
            print ("building cache - have patience")
            self.lc.populate_cache(dry_run=False)

    def setUp(self):
        self.lc = LC()

    def test_lc_find_all_with_no_list(self):
        candidates = [v[0] for v in self.lc.addresses.values()]
        chosen = lc_choose_public_address(candidates, [], self.lc)
        self.assertEqual(chosen["uuid"], "3ff12bd9-7a5a-4837-9137-cb126c53f6ea")

    def test_lc_find_all_with_list(self):
        candidates = [v[0] for v in self.lc.addresses.values()]
        chosen = lc_choose_public_address(candidates, ['ra865555-58b5-327d-e7dc-2990b0d28ff9'], self.lc)
        self.assertEqual(chosen["uuid"], "1ff12bd9-7a5a-4837-9137-cb126c53f6ea")
        pass
