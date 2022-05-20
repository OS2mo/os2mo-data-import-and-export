from datetime import datetime
from typing import Optional
from unittest import mock
from unittest import TestCase

from hypothesis import given
from hypothesis import strategies
from parameterized import parameterized

from ..lora_cache import LoraCache

# Some tests can't handle '\n', though the program can.
st_text = strategies.text().filter(lambda x: "\n" not in x)


class _TestableLoraCache(LoraCache):
    def _load_settings(self):
        return {"mox.base": "bogus://"}

    def _read_org_uuid(self):
        return "not-a-mo-org-uuid"


class _TestableLoraCacheMockedLookup(_TestableLoraCache):
    def __init__(self, lookup_response, classes=None):
        super().__init__()
        self._lookup_response = lookup_response
        self.classes = classes or {}

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


class _TestLoraCacheMethodHelper:
    """Helper mixin making it easier to test `LoraCache._cache_lora_*` methods."""

    # These are overriden by subclasses
    method_name: Optional[str] = None
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None

    def get_method_results(self, uuid, relations, attrs=None, classes=None):
        instance = _TestableLoraCacheMockedLookup(
            # Return enough of the actual LoRa response to make `_cache_lora_*` methods
            # continue to the code under test.
            # As we patch `lora_utils.get_effects`, we don't need to provide actual data
            # in "registreringer".
            [{"id": uuid, "registreringer": [None]}],
            classes=classes,
        )

        # Mock return value of `lora_utils.get_effects`
        default_list = [{"uuid": "uuid"}]
        names = (
            "tilknyttedebrugere",
            "tilknyttedeenheder",
            "organisatoriskfunktionstype",
            "primær",
        )
        collected_relations = {
            name: relations.get(name, default_list) for name in names
        }
        for additional in ("adresser", "opgaver", "tilknyttedeitsystemer"):
            if additional in relations:
                collected_relations[additional] = relations[additional]

        effects = [
            (
                self.from_dt,
                self.to_dt,
                {"attributter": attrs or {}, "relationer": collected_relations},
            )
        ]

        with mock.patch("lora_utils.get_effects", return_value=effects):
            method = getattr(instance, self.method_name)
            return method()


class TestCacheLoraManagers(_TestLoraCacheMethodHelper, TestCase):
    method_name = "_cache_lora_managers"
    from_dt = datetime(2020, 1, 1)
    to_dt = datetime(2021, 1, 1)

    _manager_uuid = "manager-uuid"
    _opgave_uuid = "opgave-uuid"

    def test_handles_empty_opgaver_rel(self):
        # Regression test for #47456, problem 1 (UnboundLocalError)
        managers = self.get_method_results(self._manager_uuid, {"opgaver": []})
        self.assertIsNone(managers[self._manager_uuid][0]["manager_level"])

    def test_handles_empty_rels(self):
        # Regression test for #47456, problem 2 (IndexError)
        managers = self.get_method_results(
            self._manager_uuid,
            {
                "tilknyttedebrugere": [],
                "tilknyttedeenheder": [],
                "organisatoriskfunktionstype": [],
                "opgaver": [{"objekttype": "lederniveau", "uuid": self._opgave_uuid}],
            },
        )
        self.assertIsNotNone(managers[self._manager_uuid][0]["manager_level"])
        self.assertIsNone(managers[self._manager_uuid][0]["user"])
        self.assertIsNone(managers[self._manager_uuid][0]["unit"])
        self.assertIsNone(managers[self._manager_uuid][0]["manager_type"])


class TestCacheLoraAddress(_TestLoraCacheMethodHelper, TestCase):
    method_name = "_cache_lora_address"
    from_dt = datetime(2020, 1, 1)
    to_dt = datetime(5027, 1, 1)  # to_dt must be in the future, this should be safe ;)

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
        address = self.get_method_results(
            self._address_uuid,
            {
                "tilknyttedebrugere": [{"uuid": self._bruger_uuid}],
                "tilknyttedeenheder": [],
                "organisatoriskfunktionstype": [{"uuid": "type_uuid"}],
                "adresser": [{"urn": f"{urn}{value}", "objekttype": scope}],
            },
        )
        self.assertEqual(address[self._address_uuid][0]["value"], value)

    @given(st_text, st_text)
    def test_address_multifield(self, value1, value2):
        address = self.get_method_results(
            self._address_uuid,
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
            },
        )
        expected = f"{value1} :: {value2}"
        self.assertEqual(address[self._address_uuid][0]["value"], expected)


class TestLoraCacheAssociations(_TestLoraCacheMethodHelper, TestCase):
    method_name = "_cache_lora_associations"
    from_dt = datetime(2020, 1, 1)
    to_dt = datetime(5020, 1, 1)

    _association_uuid = "association-uuid"
    _attrs = {
        "organisationfunktionegenskaber": [{"brugervendtnoegle": "some-user-key"}]
    }
    _it_user_uuid = "it-user-uuid"
    _primary_type_uuid = "primary-type-uuid"

    @parameterized.expand(
        [
            (None, None),
            ("primary", True),
            ("non-primary", False),
        ]
    )
    def test_handles_it_associations(
        self, primary_type_user_key, expected_primary_boolean
    ):
        relations = {"tilknyttedeitsystemer": [{"uuid": self._it_user_uuid}]}
        classes = None

        if primary_type_user_key:
            relations["primær"] = [{"uuid": self._primary_type_uuid}]
            classes = {self._primary_type_uuid: {"user_key": primary_type_user_key}}

        associations = self.get_method_results(
            self._association_uuid,
            relations,
            attrs=self._attrs,
            classes=classes,
        )
        assoc = associations[self._association_uuid][0]
        self.assertEqual(assoc["it_user"], self._it_user_uuid)
        self.assertEqual(assoc["primary_boolean"], expected_primary_boolean)

    def test_handles_empty_association_type(self):
        associations = self.get_method_results(
            self._association_uuid,
            {"organisatoriskfunktionstype": []},
            attrs=self._attrs,
        )
        assoc = associations[self._association_uuid][0]
        self.assertIsNone(assoc["association_type"])
