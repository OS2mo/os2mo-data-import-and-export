from datetime import datetime
from unittest import mock
from unittest import TestCase

from hypothesis import assume
from hypothesis import given
from hypothesis import strategies
from parameterized import parameterized

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


class TestCacheLoraAddress(TestCase):
    _address_uuid = "address-uuid"
    _bruger_uuid = "bruger-uuid"

    @parameterized.expand(
        [
            ("urn:mailto:", "EMAIL"),
            ("urn:magenta.dk:www:", "WWW"),
            ("urn:magenta.dk:telefon:", "PHONE"),
            ("urn:dk:cvr:produktionsenhed:", "PNUMBER"),
            ("urn:magenta.dk:ean:", "EAN"),
            ("urn:text:", "TEXT"),
        ]
    )
    @given(strategies.text())
    def test_get_address(self, urn, scope, value):
        address = self._get_results(
            {
                "tilknyttedebrugere": [{"uuid": self._bruger_uuid}],
                "tilknyttedeenheder": [],
                "organisatoriskfunktionstype": [{"uuid": "type_uuid"}],
                "adresser": [{"urn": f"{urn}{value}", "objekttype": scope}],
            }
        )

        self.assertEqual(address[self._address_uuid][0]["value"], value)

    @given(strategies.text(), strategies.text())
    def test_address_multifield(self, value1, value2):
        # Tests can't handle '\n', though the program can.
        assume("\n" not in value1)
        assume("\n" not in value2)
        address = self._get_results(
            {
                "tilknyttedebrugere": [{"uuid": self._bruger_uuid}],
                "tilknyttedeenheder": [],
                "organisatoriskfunktionstype": [{"uuid": "type_uuid"}],
                "adresser": [
                    {
                        "urn": f"urn:multifield_text:{value1}",
                        "objekttype": "MULTIFIELD_TEXT",
                    },
                    {
                        "urn": f"urn:multifield_text2:{value2}",
                        "objekttype": "MULTIFIELD_TEXT",
                    },
                ],
            }
        )
        expected = f"{value1} :: {value2}"
        self.assertEqual(address[self._address_uuid][0]["value"], expected)

    def _get_results(self, relations):
        instance = _TestableLoraCacheMockedLookup(
            # Return enough of the actual LoRa response to make
            # `_cache_lora_managers` continue to offending code. As we patch
            # `lora_utils.get_effects`, we don't need to provide actual data in
            # "registreringer".
            [{"id": self._address_uuid, "registreringer": [None]}]
        )

        # Mock return value of `lora_utils.get_effects`
        default_list = [{"uuid": "uuid"}]
        effect = (
            datetime(2020, 1, 1),
            datetime(
                5027, 1, 1
            ),  # todate must be in the future, this should be safe ;)
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
                    "adresser": relations.get("adresser", default_list),
                }
            },
        )

        with mock.patch("lora_utils.get_effects", return_value=[effect]):
            return instance._cache_lora_address()
