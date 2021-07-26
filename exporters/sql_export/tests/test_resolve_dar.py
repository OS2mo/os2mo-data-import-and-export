import logging
import unittest
from unittest.mock import call, patch
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis.strategies import booleans, lists, uuids

import exporters.sql_export.lora_cache
from exporters.sql_export.lora_cache import LoraCache


class LoraCacheTest(LoraCache):
    """Subclass to override methods with side-effects."""

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
        lc = LoraCacheTest(settings={"Dummy": True})
        self.assertEqual(lc.resolve_dar, True)
        self.assertEqual(lc.dar_map, {})

    @given(booleans())
    def test_cache_dar_empty(self, resolve_dar):
        """With empty dar_map, resolve does not matter."""
        lc = LoraCacheTest(resolve_dar, settings={"Dummy": True})
        self.assertEqual(lc.resolve_dar, resolve_dar)
        self.assertEqual(lc.dar_map, {})

        with self._caplog.at_level(logging.INFO):
            dar_cache = lc._cache_dar()
            self.assertEqual(self.get_last_log(), "Total dar: 0, no-hit: 0")
        self.assertEqual(dar_cache, {})

    @patch("exporters.sql_export.lora_cache.dar_helper.sync_dar_fetch")
    @given(booleans(), lists(uuids()))
    def test_cache_dar(self, sync_dar_fetch, resolve_dar, dar_uuids):
        """With filled dar_map, resolve does matter."""
        sync_dar_fetch.reset_mock()
        # Mock sync_dar_fetch to be noop
        def noop_sync_dar_fetch(dar_uuids, addrtype="adresser"):
            return {}, dar_uuids

        sync_dar_fetch.side_effect = noop_sync_dar_fetch

        # Setup LoraCache Object
        lc = LoraCacheTest(resolve_dar, settings={"Dummy": True})
        self.assertEqual(lc.resolve_dar, resolve_dar)
        self.assertEqual(lc.dar_map, {})
        lc.addresses = {}

        # Prepare dar_map with provided dar_uuids
        num_uuids = len(dar_uuids)
        dar_uuids = list(map(str, dar_uuids))
        for dar_uuid in dar_uuids:
            lc.dar_map[dar_uuid] = []

        # Fire the call and check log output
        with self._caplog.at_level(logging.INFO):
            dar_cache = lc._cache_dar()
            expected_log_message = f"Total dar: {num_uuids}, no-hit: {num_uuids}"
            self.assertEqual(self.get_last_log(), expected_log_message)

        # Ensure that sync_dar_fetch is only called when resolve_dar is True
        if resolve_dar:
            calls = [
                call(lc.dar_map.keys()),
                call(dar_uuids, addrtype="adgangsadresser"),
            ]
            sync_dar_fetch.assert_has_calls(calls)
        else:
            sync_dar_fetch.assert_not_called()

        # Check that all our betegnelser has been set
        for dar_uuid in dar_uuids:
            self.assertIn(dar_uuid, dar_cache)
            self.assertEqual(dar_cache[dar_uuid], {"betegnelse": None})
