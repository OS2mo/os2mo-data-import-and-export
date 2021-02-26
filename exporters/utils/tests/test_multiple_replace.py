from functools import reduce
from unittest import TestCase

import hypothesis.strategies as st
from hypothesis import assume, event, example, given

from exporters.utils.multiple_replace import multiple_replace

from exporters.utils.multiple_replace import multiple_replace


class MultipleReplaceTests(TestCase):
    @given(st.text())
    def test_no_replace(self, text):
        """Test that no replace array yields noop."""
        self.assertEqual(multiple_replace({}, text), text)

    @given(st.text())
    def test_empty_string_replace(self, text):
        """Test that no replace array yields noop."""
        with self.assertRaises(AssertionError):
            multiple_replace({"": "spam"}, text)

    @given(st.text(), st.text(min_size=1), st.text())
    @example("I like tea", "tea", "coffee")  # --> I like coffee
    def test_replace_single_as_replace(self, text, before, after):
        """Test that single replacement works as str.replace."""
        new_text = text.replace(before, after)
        event("new_text == text: " + str(new_text == text))

        self.assertEqual(multiple_replace({before: after}, text), new_text)

    def test_replace_multiple_interference(self):
        """Test that multiple replacement does not necessarily work as str.replace.

        I.e. chained invokations of str.replace may replace something that was
        already replaced. Creating undesirable cycles.
        """
        text = "I love eating"
        changes = [("I", "love"), ("love", "eating"), ("eating", "spam")]

        new_text = reduce(lambda text, change: text.replace(*change), changes, text)
        self.assertEqual(new_text, "spam spam spam")

        text = multiple_replace(dict(changes), text)
        self.assertEqual(text, "love eating spam")

    @given(st.text(), st.dictionaries(st.text(min_size=1), st.text()))
    def test_replace_multiple_as_replace(self, text, changes):
        """Test that multiple replacement works as str.replace.

        This only applies when interference does not come into play.
        """
        # Protect against interference (unlikely to occur)
        for value in changes.values():
            for key in changes.keys():
                assume(value not in key)

        new_text = reduce(
            lambda text, change: text.replace(*change), changes.items(), text
        )
        event("new_text == text: " + str(new_text == text))

        self.assertEqual(multiple_replace(changes, text), new_text)
