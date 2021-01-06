from unittest import TestCase
from unittest.mock import MagicMock, call

import hypothesis.strategies as st
from exporters.utils.progress import (format_progress_iterator,
                                      print_progress_iterator, progress_iterator)
from hypothesis import assume, given
from more_itertools import consume


def noop(*args, **kwargs):
    """Noop outputter."""
    pass


class ProgressIteratorTests(TestCase):
    @given(st.lists(st.integers()))
    def test_element_passthrough_list(self, listy):
        """Test that elements are passed through without modification."""
        progressed_list = list(progress_iterator(listy, noop))
        self.assertEqual(progressed_list, listy)

        progressed_list = list(format_progress_iterator(listy, noop))
        self.assertEqual(progressed_list, listy)

        progressed_list = list(print_progress_iterator(listy))
        self.assertEqual(progressed_list, listy)

    @given(st.lists(st.integers()))
    def test_element_passthrough_iterator(self, listy):
        """Test that elements are passed through without modification."""
        length = len(listy)

        itera = iter(listy)
        progressed_list = list(progress_iterator(itera, noop, total=length))
        self.assertEqual(progressed_list, listy)

        itera = iter(listy)
        progressed_list = list(format_progress_iterator(itera, noop, total=length))
        self.assertEqual(progressed_list, listy)

        itera = iter(listy)
        progressed_list = list(print_progress_iterator(itera, total=length))
        self.assertEqual(progressed_list, listy)

    @given(st.lists(st.integers()))
    def test_outputter_calls_mod_1(self, listy):
        """Test that outputter is called for every element with mod=1."""
        length = len(listy)

        calls = [call(x + 1, length) for x in range(length)]

        outputter = MagicMock()
        consume(progress_iterator(listy, outputter, mod=1))
        outputter.assert_has_calls(calls)

    @given(st.lists(st.integers(), min_size=2))
    def test_outputter_calls_mod_0(self, listy):
        """Test that outputter cannot be called with mod=0."""
        outputter = MagicMock()
        with self.assertRaises(ZeroDivisionError):
            consume(progress_iterator(listy, outputter, mod=0))

    @given(st.lists(st.integers()), st.integers())
    def test_outputter_calls_any_mod(self, listy, mod):
        """Test that outputter is called for correct elements."""
        assume(mod != 0)

        length = len(listy)

        def gen_current(length, mod):
            if length == 0:
                return
            if length == 1:
                yield 1
                return

            yield 1
            for x in range(1, length - 1):
                x += 1
                if x % mod == 0:
                    yield x
            yield length

        calls = [call(current, length) for current in gen_current(length, mod)]

        outputter = MagicMock()
        consume(progress_iterator(listy, outputter, mod=mod))
        outputter.assert_has_calls(calls)

    @given(st.lists(st.integers()))
    def test_outputter_calls_are_progressive(self, listy):
        """Test that outputter is called only as iteration happen."""
        length = len(listy)

        outputter = MagicMock()
        iterator = progress_iterator(listy, outputter, mod=1)

        # Consume just one element, and check that the mock has 1 call
        for x in range(length):
            consume(iterator, 1)
            outputter.assert_has_calls([call(x + 1, length)])
