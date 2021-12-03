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
    _manager_uuid = "manager-uuid"
    _opgave_uuid = "opgave-uuid"

    def test_handles_empty_opgaver_rel(self):
        # Regression test for #47456, problem 1 (UnboundLocalError)
        managers = self._get_results({"opgaver": []})
        self.assertIsNone(managers[self._manager_uuid][0]["manager_level"])

    def test_handles_empty_rels(self):
        # Regression test for #47456, problem 2 (IndexError)
        managers = self._get_results(
            {
                "tilknyttedebrugere": [],
                "tilknyttedeenheder": [],
                "organisatoriskfunktionstype": [],
                "opgaver": [{"objekttype": "lederniveau", "uuid": self._opgave_uuid}],
            }
        )
        self.assertIsNotNone(managers[self._manager_uuid][0]["manager_level"])
        self.assertIsNone(managers[self._manager_uuid][0]["user"])
        self.assertIsNone(managers[self._manager_uuid][0]["unit"])
        self.assertIsNone(managers[self._manager_uuid][0]["manager_type"])

    def _get_results(self, relations):
        instance = _TestableLoraCacheMockedLookup(
            # Return enough of the actual LoRa response to make
            # `_cache_lora_managers` continue to offending code. As we patch
            # `lora_utils.get_effects`, we don't need to provide actual data in
            # "registreringer".
            [{"id": self._manager_uuid, "registreringer": [None]}]
        )

        # Mock return value of `lora_utils.get_effects`
        default_list = [{"uuid": "uuid"}]
        effect = (
            datetime(2020, 1, 1),
            datetime(2021, 1, 1),
            {
                "relationer": {
                    "tilknyttedebrugere": relations.get(
                        "tilknyttedebrugere", default_list
                    ),
                    "tilknyttedeenheder": relations.get(
                        "tilknyttedeenheder", default_list
                    ),
                    "organisatoriskfunktionstype": relations.get(
                        "organisatoriskfunktionstype", default_list
                    ),
                    "opgaver": relations.get("opgaver", default_list),
                }
            },
        )

        with mock.patch("lora_utils.get_effects", return_value=[effect]):
            return instance._cache_lora_managers()
