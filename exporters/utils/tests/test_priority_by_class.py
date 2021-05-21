from operator import itemgetter
from random import shuffle
from unittest import TestCase

from parameterized import parameterized

from exporters.sql_export.lora_cache import LoraCache
from rautils.priority_by_class import lc_choose_public_address


class LC:
    classes = {
        # Visibility
        "90b11c00-0000-0000-0000-000000000000": {
            "scope": "PUBLIC",
        },
        "5ec4e100-0000-0000-0000-000000000000": {
            "scope": "SECRET",
        },
        # Adresse typer
        "0ff1ce00-0000-0000-0000-000000000000": {
            "scope": "OFFICE",
        },
        "1110b11e-0000-0000-0000-000000000000": {
            "scope": "PHONE",
        },
        "911011e0-0000-0000-0000-000000000000": {
            "scope": "PHONE",
        },
        "deadbeef-0000-0000-0000-000000000000": {
            "scope": "STEAK",
        },
    }

    addresses = {
        # Mobile Phone - Secret
        "11111111-1111-1111-1111-111111111111": [
            {
                "uuid": "11111111-1111-1111-1111-111111111111",
                "adresse_type": "1110b11e-0000-0000-0000-000000000000",
                "value": "emailsecret@example.com",
                "visibility": "5ec4e100-0000-0000-0000-000000000000",
            },
        ],
        # Office - Unset
        "22222222-2222-2222-2222-222222222222": [
            {
                "uuid": "22222222-2222-2222-2222-222222222222",
                "adresse_type": "0ff1ce00-0000-0000-0000-000000000000",
                "value": "231",
                "visibility": None,
            },
        ],
        # Phone - Public
        "33333333-3333-3333-3333-333333333333": [
            {
                "uuid": "33333333-3333-3333-3333-333333333333",
                "adresse_type": "911011e0-0000-0000-0000-000000000000",
                "value": "+45 88888888",
                "visibility": "90b11c00-0000-0000-0000-000000000000",
            },
        ],
        # Phone - Secret
        "44444444-4444-4444-4444-444444444444": [
            {
                "uuid": "44444444-4444-4444-4444-444444444444",
                "adresse_type": "911011e0-0000-0000-0000-000000000000",
                "value": "+45 70101155",
                "visibility": "5ec4e100-0000-0000-0000-000000000000",
            },
        ],
        # Phone - Unset
        "55555555-5555-5555-5555-555555555555": [
            {
                "uuid": "55555555-5555-5555-5555-555555555555",
                "adresse_type": "911011e0-0000-0000-0000-000000000000",
                "value": "+45 116111",
                "visibility": None,
            },
        ],
        # Mobile Phone - Public
        "66666666-6666-6666-6666-666666666666": [
            {
                "uuid": "66666666-6666-6666-6666-666666666666",
                "adresse_type": "1110b11e-0000-0000-0000-000000000000",
                "value": "emailpublic@example.com",
                "visibility": "90b11c00-0000-0000-0000-000000000000",
            },
        ],
    }


class PriorityByClassTests(TestCase):
    def setUp(self):
        self.lc = LC()
        self.candidates = list(map(itemgetter(0), self.lc.addresses.values()))

    def get_candidate_uuid(self, priority, reverse=False, shuffled=False):
        candidates = self.candidates
        if reverse:
            candidates = reversed(candidates)
        if shuffled:
            tmp_list = candidates[:]
            shuffle(tmp_list)
            candidates = tmp_list
        return lc_choose_public_address(candidates, priority, self.lc)["uuid"]

    @parameterized.expand(
        [
            ## No priority, we expect first valid element
            # First valid is 2, reversed it is 4
            [[], "2"],
            ## Priority invalid class, we expect first valid element
            # First valid is 2, reversed it is 4
            [["cafebabe-0000-0000-0000-000000000000"], "2"],
            ## Priority not found, we expect first valid element
            # First valid is 2, reversed it is 4
            [["deadbeef-0000-0000-0000-000000000000"], "2"],
            ## Priority found (Phone) (2 matches), we expect first of type
            [["911011e0-0000-0000-0000-000000000000"], "3"],
            ## Priority found (Office) (1 match), we expect first of type
            [["0ff1ce00-0000-0000-0000-000000000000"], "2"],
            ## Priority found (Mobile Phone) (1 match), we expect first of type
            [["1110b11e-0000-0000-0000-000000000000"], "6"],
            ## Priority found (Phone + Mobile) (3 matches), we expect first of phone
            [
                [
                    "911011e0-0000-0000-0000-000000000000",
                    "1110b11e-0000-0000-0000-000000000000",
                ],
                "3",
            ],
            ## Priority found (Unknown + Office) (1 matches), we expect first of office
            [
                [
                    "deadbeef-0000-0000-0000-000000000000",
                    "0ff1ce00-0000-0000-0000-000000000000",
                ],
                "2",
            ],
        ]
    )
    def test_computed_match(self, priority, expected):
        """We expect to get the most desirable candidate.

        Reordering the list of candidates should not change anything.
        """
        uuid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".replace("x", expected)
        # Check with candidate as is
        chosen = self.get_candidate_uuid(priority)
        self.assertEqual(chosen, uuid)
        # Check with reversed candidates
        chosen = self.get_candidate_uuid(priority, reverse=True)
        self.assertEqual(chosen, uuid)
        # Check with shuffled list
        chosen = self.get_candidate_uuid(priority, shuffled=True)
        self.assertEqual(chosen, uuid)
