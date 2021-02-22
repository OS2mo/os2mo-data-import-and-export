from functools import partial
from unittest import TestCase

import hypothesis.strategies as st
from hypothesis import given

from exporters.utils.lazy_dict import LazyDict, LazyEval


def exception_func():
    """Function that always throws an exception."""
    raise ValueError()


def identity_func(x):
    """Function that always returns its argument."""
    return x


class LazyDictTests(TestCase):
    """Test LazyDict functions as expected."""

    def setUp(self):
        self.counter = 0

        def counter_func():
            self.counter += 1
            return self.counter

        self.counter_func = counter_func

    @given(st.dictionaries(st.text(), st.text()))
    def test_ordinary_dict_functionality(self, dicty: dict):
        """Test that LazyDict functions similar to an ordinary dict."""
        lazy_dict = LazyDict(dicty)

        self.assertEqual(dicty.items(), lazy_dict.items())
        self.assertEqual(len(dicty), len(lazy_dict))
        self.assertEqual(repr(dicty), repr(lazy_dict))

        for key in dicty.keys():
            self.assertEqual(dicty[key], lazy_dict[key])

    def test_lazy_evalulation(self):
        """Test that LazyDict supports lazy evaluation using LazyEval."""
        lazy_dict = LazyDict({"exception_func": LazyEval(exception_func)})
        with self.assertRaises(ValueError):
            lazy_dict["exception_func"]

        lazy_dict["identity_func1"] = LazyEval(partial(lambda x: x, 2))
        self.assertEqual(lazy_dict["identity_func1"], 2)
        self.assertEqual(lazy_dict["identity_func1"], 2)

        lazy_dict["identity_func2"] = LazyEval(partial(identity_func, 4))
        self.assertEqual(lazy_dict["identity_func2"], 4)
        self.assertEqual(lazy_dict["identity_func2"], 4)

        lazy_dict["identity_func3"] = LazyEval(lambda: 8)
        self.assertEqual(lazy_dict["identity_func3"], 8)
        self.assertEqual(lazy_dict["identity_func3"], 8)

        lazy_dict["c_no_cache"] = LazyEval(self.counter_func)
        self.assertEqual(lazy_dict["c_no_cache"], 1)
        self.assertEqual(lazy_dict["c_no_cache"], 1)

        lazy_dict["c_cache"] = LazyEval(self.counter_func, cache=False)
        self.assertEqual(lazy_dict["c_cache"], 2)
        self.assertEqual(lazy_dict["c_cache"], 3)
        self.assertEqual(lazy_dict["c_no_cache"], 1)
        self.assertEqual(lazy_dict["c_cache"], 4)
