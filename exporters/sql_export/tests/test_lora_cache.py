from datetime import datetime
from unittest import mock
from unittest import TestCase

from ..lora_cache import LoraCache


class _TestableLoraCache(LoraCache):
    def _load_settings(self):
        return {"mox.base": "bogus://"}

    def _read_org_uuid(self):
        return "not-a-mo-org-uuid"


class _TestableLoraCacheMockedLookup(_TestableLoraCache):
    def __init__(self, lookup_response):
        super().__init__()
        self._lookup_response = lookup_response

    def _perform_lora_lookup(self, url, params, **kwargs):
        return self._lookup_response


class _SpecificException(Exception):
    pass


class TestPerformLoraLookup(TestCase):
    def test_retry(self):
        # Test that a failing GET request is retried the number of times set in
        # `@retry(stop_max_attempt_number=7)`
        instance = _TestableLoraCache()
        num_retries = 7
        # Eventually, the exception bubbles up from the retry logic. Catch it
        # here, so we can verify the call count, etc.
        with self.assertRaises(_SpecificException):
            with mock.patch("requests.get", side_effect=_SpecificException()) as _get:
                instance._perform_lora_lookup("not-an-url", {})
            _get.assert_called()
            self.assertEqual(_get.call_count, num_retries)


class TestCacheLoraManagers(TestCase):
    def test_handles_empty_opgaver_rel(self):
        # Regression test for #47456

        instance = _TestableLoraCacheMockedLookup(
            # Return enough of the actual LoRa response to make
            # `_cache_lora_managers` continue to offending code. As we patch
            # `lora_utils.get_effects`, we don't need to provide actual data in
            # "registreringer".
            [{"id": "manager_uuid", "registreringer": [None]}]
        )

        # Mock return value of `lora_utils.get_effects`
        effect = (
            datetime(2020, 1, 1),
            datetime(2021, 1, 1),
            {
                "relationer": {
                    "tilknyttedeenheder": [{"uuid": "uuid"}],
                    "organisatoriskfunktionstype": [{"uuid": "uuid"}],
                    "opgaver": [],
                }
            },
        )

        with mock.patch("lora_utils.get_effects", return_value=[effect]):
            managers = instance._cache_lora_managers()
            self.assertIn("manager_uuid", managers)
            self.assertIsNone(managers["manager_uuid"][0]["manager_level"])
