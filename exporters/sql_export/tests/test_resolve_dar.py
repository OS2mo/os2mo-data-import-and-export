import logging
import unittest
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis.strategies import booleans
from hypothesis.strategies import lists
from hypothesis.strategies import uuids

from exporters.sql_export.lora_cache import LoraCache


class LoraCacheTest(LoraCache):
    """Subclass to override methods with side-effects."""

    def _load_settings(self):
        """We want to avoid reading settings.json."""
        return {}

    def _read_org_uuid(self):
        """We want to avoid MO lookups."""
        pass


class TestResolveDar(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        """Enable log capturing."""
        self._caplog = caplog

    def get_last_log(self):
        """Return the last log message emitted."""
        return self._caplog.records[-1].message

    def test_init(self):
        """Ensure we can create LoraCacheTest without issues."""
        lc = LoraCacheTest()
        self.assertEqual(lc.resolve_dar, True)
        self.assertEqual(lc.dar_map, {})

    @given(booleans())
    def test_cache_dar_empty(self, resolve_dar):
        """With empty dar_map, resolve does not matter."""
        lc = LoraCacheTest(resolve_dar)
        self.assertEqual(lc.resolve_dar, resolve_dar)
        self.assertEqual(lc.dar_map, {})

        with self._caplog.at_level(logging.INFO):
            dar_cache = lc._cache_dar()
            self.assertEqual(self.get_last_log(), "Total dar: 0, no-hit: 0")
        self.assertEqual(dar_cache, {})

    @given(booleans(), lists(uuids()))
    def test_cache_dar(self, resolve_dar, dar_uuids):
        """With filled dar_map, resolve does matter."""
        lc = LoraCacheTest(resolve_dar=resolve_dar)
        self.assertEqual(lc.resolve_dar, resolve_dar)
        self.assertEqual(lc.dar_map, {})
        lc.addresses = {}

        # Prepare dar_map with provided dar_uuids
        num_uuids = len(dar_uuids)
        dar_uuids = set(map(str, dar_uuids))
        for dar_uuid in dar_uuids:
            lc.dar_map[dar_uuid] = []

        # Fire the call and check log output
        with patch.object(
            lc, "_read_from_dar", return_value=({}, dar_uuids)
        ) as sync_dar_fetch:
            with self._caplog.at_level(logging.INFO):
                dar_cache = lc._cache_dar()
                expected_log_message = f"Total dar: {num_uuids}, no-hit: {num_uuids}"
                assert self.get_last_log() == expected_log_message

        # Ensure that sync_dar_fetch is only called when resolve_dar is True
        if resolve_dar:
            sync_dar_fetch.assert_called_once_with(dar_uuids)
        else:
            sync_dar_fetch.assert_not_called()

        # Check that all our betegnelser has been set
        for dar_uuid in dar_uuids:
            self.assertIn(str(dar_uuid), dar_cache)
            self.assertEqual(dar_cache[dar_uuid], {"betegnelse": None})
