from typing import Tuple
from unittest import TestCase

from parameterized import parameterized

from exporters.utils.jinja_filter import create_filter, string_to_bool


class StringToBoolTests(TestCase):
    """Test the string_to_bool function works as expected."""

    @parameterized.expand(
        [
            ["yes", True],
            ["Yes", True],
            ["YES", True],
            ["yEs", True],
            ["no", False],
            ["No", False],
            ["NO", False],
            ["YESYES", False],
            ["yes or no", False],
            ["True", True],
            ["true", True],
            ["TRuE", True],
            ["TRUE", True],
            ["false", False],
            ["False", False],
            ["FAlsE", False],
            ["FALSE", False],
            ["TrueTrue", False],
            ["true or false", False],
            ["1", True],
            ["1.0", True],
            ["0", False],
            ["0.0", False],
            ["!", False],
            ["I", False],
            ["1 or 0", False],
            ["10", False],
            ["01", False],
            ["1.1", False],
            ["sand", False],
            ["sandt", False],
            ["t", False],
            ["[]", False],
            ["{}", False],
            ["", False],
            ["Quality Code", False],
            ["Melatonin", False],
            ["Pineapple belongs on Pizza", False],
        ]
    )
    def test_string_to_bool(self, string: str, expected: bool):
        result = string_to_bool(string)
        self.assertEqual(result, expected)


class CreateFilterTests(TestCase):
    @parameterized.expand(
        [
            ["TRUE", True],
            ["FALSE", False],
            ["{{ 1 == 1 }}", True],
            ["{{ 0 == 1 }}", False],
            ["{{ 2 ** 2 == 4 }}", True],
            ["{{ 2 + 5 == 3 + 4 }}", True],
            ["{{ 2 + 9 == 11 - 1 }}", False],
            ["{{ 1 + 0 }}", True],
            ["{{ 0.5 + 0.5 }}", True],
            ["{{ 0 + 0 }}", False],
            ["{{ 0.5 * 0 }}", False],
        ]
    )
    def test_create_filter_no_arguments(self, jinja_string: str, expected: bool):
        result = create_filter(jinja_string, [])([])
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            ["{{ inty - 6 }}", True],
            ["{{ inty + 3 }}", False],
            ["{{ floaty + inty > 0 }}", True],
            ["{{ floaty + inty < 0 }}", False],
            ["{{ inty - floaty > 0 }}", True],
            ["{{ inty - floaty < 0 }}", False],
            ["{{ inty * floaty }}", False],
        ]
    )
    def test_create_filter_numbers(self, jinja_string: str, expected: bool):
        result = create_filter(jinja_string, ["inty", "floaty"])([7, 3.14])
        self.assertEqual(result, expected)
