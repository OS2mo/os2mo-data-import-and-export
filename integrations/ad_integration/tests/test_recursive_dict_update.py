import unittest

from parameterized import parameterized

from ..utils import recursive_dict_update


class TestRecursiveDictUpdate(unittest.TestCase):
    @parameterized.expand(
        [
            [
                # Original
                {"alfa": 1},
                # Updates
                {"alfa": 2},
                # Non recursive update
                {"alfa": 2},
                # Recursive update
                {"alfa": 2},
            ],
            [
                # Original
                {"alfa": {"beta": 2, "charlie": 3}},
                # Updates
                {"alfa": {"beta": 4}},
                # Non recursive update
                {"alfa": {"beta": 4}},
                # Recursive update
                {"alfa": {"beta": 4, "charlie": 3}},
            ],
            [
                # Original
                {"alfa": {"beta": {"echo": 4, "fox": 5}, "charlie": 3}},
                # Updates
                {"alfa": {"beta": {"echo": 6}}},
                # Non recursive update
                {"alfa": {"beta": {"echo": 6}}},
                # Recursive update
                {"alfa": {"beta": {"echo": 6, "fox": 5}, "charlie": 3}},
            ],
            [
                # Original
                {"alfa": {"beta": {"echo": 4}, "charlie": {"fox": 5}}},
                # Updates
                {"alfa": {"beta": {"echo": 6}}},
                # Non recursive update
                {"alfa": {"beta": {"echo": 6}}},
                # Recursive update
                {"alfa": {"beta": {"echo": 6}, "charlie": {"fox": 5}}},
            ],
        ]
    )
    def test_function(self, original, updates, expected, expected_recursive):
        updated = {**original, **updates}
        r_updated = recursive_dict_update(original, updates)
        self.assertEqual(updated, expected)
        self.assertEqual(r_updated, expected_recursive)
