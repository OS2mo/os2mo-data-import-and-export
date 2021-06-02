from unittest import TestCase

import hypothesis.strategies as st
from hypothesis import given

from ..ad_reader import first_included

best_match = {
    "SamAccountName": "best_match",
    "xFieldname": "A",
}
match = {
    "SamAccountName": "match",
    "xFieldname": "C",
}
no_match = {
    "SamAccountName": "no_match",
    "xFieldname": "X",
}


class TestFirstIncluded(TestCase):
    def setUp(self):
        self.settings = {
            "discriminator.field": "xFieldname",
            "discriminator.values": ["A", "B", "C"],
            "discriminator.function": "include",
        }

    @given(st.permutations([best_match, match, no_match]))
    def test_first_included_user_order_irrelevant(self, users):
        result = first_included(self.settings, users)
        self.assertEqual(result, best_match)

    def test_substring_bug(self):
        # This users xFieldName is not even in discriminator.values
        # They should therefore not be included
        unwanted_match = {
            "SamAccountName": "unwanted_match",
            "xFieldname": "Adversary",
        }
        result = first_included(self.settings, [unwanted_match, match])
        self.assertEqual(result, match)
