from datetime import timedelta
from time import sleep
from unittest import TestCase

import hypothesis.strategies as st
from hypothesis import given, settings

from exporters.utils.catchtime import catchtime


class CatchtimeTests(TestCase):
    """Test the catchtime contextmanager works as expected."""

    @settings(max_examples=15, deadline=timedelta(milliseconds=1100))
    @given(st.floats(min_value=0.2, max_value=1))
    def test_catchtime(self, sleep_time: float):
        """Test that catchtime returns the expected time."""
        with catchtime() as t:
            sleep(sleep_time)
        time_spent = t()

        self.assertLess(time_spent - sleep_time, 0.01)

    @settings(max_examples=15, deadline=timedelta(milliseconds=1100))
    @given(st.floats(min_value=0.2, max_value=1))
    def test_catchtime_process(self, sleep_time: float):
        """Test that catchtime returns the expected time and process time."""
        with catchtime(include_process_time=True) as t:
            sleep(sleep_time)
        time_spent, process_time = t()

        self.assertLess(time_spent - sleep_time, 0.01)
        self.assertLess(process_time, time_spent - sleep_time)
