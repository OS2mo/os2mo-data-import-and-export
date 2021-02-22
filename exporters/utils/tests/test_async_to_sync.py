from asyncio import iscoroutinefunction
from typing import Tuple
from unittest import TestCase

import hypothesis.strategies as st
from exporters.utils.async_to_sync import async_to_sync
from hypothesis import given


@async_to_sync
async def sync_add(a: int, b: int) -> int:
    return a + b


async def async_add(a: int, b: int) -> int:
    return a + b


class AsyncToSyncTests(TestCase):
    """Test the async to sync decorator works as expected."""

    @given(st.integers(), st.integers())
    def test_add(self, a: int, b: int):
        expected = a + b

        self.assertTrue(iscoroutinefunction(async_add))
        self.assertFalse(iscoroutinefunction(sync_add))
        self.assertFalse(iscoroutinefunction(async_to_sync(async_add)))
        self.assertFalse(iscoroutinefunction(async_to_sync(sync_add)))

        result = sync_add(a, b)
        self.assertEqual(result, expected)

        result = async_to_sync(async_add)(a, b)
        self.assertEqual(result, expected)
