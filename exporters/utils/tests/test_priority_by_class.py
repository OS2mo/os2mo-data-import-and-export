from operator import itemgetter
from unittest import TestCase

from exporters.sql_export.lora_cache import LoraCache
from exporters.utils.priority_by_class import lc_choose_public_address


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
        '1ff12bd9-7a5a-4837-9137-cb126c53f6ea': [
            {
                'uuid': '1ff12bd9-7a5a-4837-9137-cb126c53f6ea',
                'adresse_type': 'ra865555-58b5-327d-e7dc-2990b0d28ff9',
                'value': 'emailunknown@example.com',
                'visibility': None
            }
        ],
    }


class PriorityByClassTests(TestCase):

    def setUp(self):
        self.lc = LC()
        self.candidates = list(map(itemgetter(0), self.lc.addresses.values()))

    def test_lc_find_all_with_no_list(self):
        """We expect to get the first valid entry."""
        # First entry is invalid (2f...), thus we expect to get second entry
        chosen = lc_choose_public_address(self.candidates, [], self.lc)
        self.assertEqual(chosen["uuid"], '3ff12bd9-7a5a-4837-9137-cb126c53f6ea')

        # Last entry (first after reverse) is valid, thus we expect to get that
        chosen = lc_choose_public_address(reversed(self.candidates), [], self.lc)
        self.assertEqual(chosen["uuid"], '1ff12bd9-7a5a-4837-9137-cb126c53f6ea')
        
    def test_lc_find_all_with_list(self):
        """We expect to get the most desirable entry."""
        # Most desirable is third entry, we expect to get that
        chosen = lc_choose_public_address(
            self.candidates, ['ra865555-58b5-327d-e7dc-2990b0d28ff9'], self.lc
        )
        self.assertEqual(chosen["uuid"], "1ff12bd9-7a5a-4837-9137-cb126c53f6ea")

        # Most desirable is first entry, we expect to get that
        chosen = lc_choose_public_address(
            reversed(self.candidates), ['ra865555-58b5-327d-e7dc-2990b0d28ff9'], self.lc
        )
        self.assertEqual(chosen["uuid"], '1ff12bd9-7a5a-4837-9137-cb126c53f6ea')
