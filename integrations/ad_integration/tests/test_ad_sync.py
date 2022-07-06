from datetime import date
from itertools import chain
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from unittest import TestCase
from unittest.mock import MagicMock

from parameterized import parameterized

from ..ad_sync import AdMoSync
from ..utils import AttrDict
from .mocks import MO_UUID
from .mocks import MockADParameterReader
from .mocks import MockLoraCache
from .mocks import MockLoraCacheEmptyEmployee
from .mocks import MockMoraHelper
from .test_utils import dict_modifier
from .test_utils import TestADMoSyncMixin


def iso_date(date):
    return date.strftime("%Y-%m-%d")


def today_iso():
    return iso_date(date.today())


MO_VALUES = {"uuid": MO_UUID}


class _TestableAdMoSync(AdMoSync):
    def _setup_settings(self, all_settings):
        self.settings = {}

    def _setup_visibilities(self):
        self.visibility = {}

    def _setup_mora_helper(self):
        return MockMoraHelper("cpr")

    def _setup_lora_cache(self):
        return None


class _TestableAdMoSyncLoraCache(_TestableAdMoSync):
    def _setup_lora_cache(self):
        return MockLoraCache(MO_VALUES)


class _TestableAdMoSyncLoraCacheEmptyEmployee(_TestableAdMoSync):
    def _setup_lora_cache(self):
        return MockLoraCacheEmptyEmployee(MO_VALUES)


class _TestableAdMoSyncLoraCacheUserAttrs(_TestableAdMoSync):
    """A subclass of `_TestableAdMoSync` which is used by
    `TestEditUserAttrsLoraCache.test_edit_user_attrs_is_idempotent` to test that calls
    to `AdMoSync._edit_user_attrs` are idempotent.
    """

    class _MockMoraHelperMutatingLoraCache(MockMoraHelper):
        """A subclass of `MockMoraHelper` which records all calls to `update_user` and
        mutates the mocked (LoraCache) user object in order to test idempotency.
        """

        def __init__(self, loracache: MockLoraCache):
            self.update_user_calls: List[Tuple[str, Dict[str, Any]]] = []
            self._loracache = loracache
            super().__init__("cpr")

        def update_user(self, uuid, data):
            # Record attempt to mutate user given by `uuid`
            self.update_user_calls.append((uuid, data))
            # Mutate the user in mock `LoraCache`
            self._loracache.users[uuid][0].update(**data)

    def __init__(self):
        self.mo_user = {"uuid": MO_UUID}
        super().__init__()

    def _setup_settings(self, all_settings):
        # Configuration keys required by `AdMoSync._update_users`
        required_keys = {
            "ad_mo_sync_pre_filters": [],
            "ad_mo_sync_terminate_disabled": None,
            "ad_mo_sync_terminate_disabled_filters": [],
            "ad_mo_sync_terminate_missing": None,
            "ad_mo_sync_terminate_missing_require_itsystem": None,
        }

        # Map AD field "UserPrincipalName" to MO field "user_key"
        self.settings = {
            "integrations.ad": [
                {
                    "ad_mo_sync_mapping": {
                        "user_attrs": {"UserPrincipalName": "user_key"},
                    },
                    **required_keys,
                }
            ]
        }

    def _setup_lora_cache(self):
        return MockLoraCache(self.mo_user)

    def _setup_mora_helper(self):
        # Return a helper mutating the mock `LoraCache` returned by `_setup_lora_cache`
        return self._MockMoraHelperMutatingLoraCache(self.lc)

    def _setup_ad_reader_and_cache_all(self, index, cache_all=True):
        # Return a mock AD reader which also exposes our mocked `user_attrs` mapping

        def _get_setting():
            return self.settings["integrations.ad"][index]

        self.mock_ad_reader = MockADParameterReader()
        self.mock_ad_reader._get_setting = _get_setting
        return self.mock_ad_reader


class TestADMoSync(TestCase, TestADMoSyncMixin):
    maxDiff = None

    def setUp(self):
        self._initialize_configuration()

    def _sync_address_mapping_transformer(self):
        def add_sync_mapping(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {
                "user_addresses": {
                    # Different visibility
                    "email": ["email_uuid", None],
                    "telephone": ["telephone_uuid", "PUBLIC"],
                    "office": ["office_uuid", "INTERNAL"],
                    "mobile": ["mobile_uuid", "SECRET"],
                    # No uuid
                    "floor": ["", None],
                }
            }
            return settings

        return add_sync_mapping

    def _setup_address_type_mappings(self, address_type):
        settings = self._prepare_settings(self._sync_address_mapping_transformer())
        mapping = settings["integrations.ad"][0]["ad_mo_sync_mapping"]
        address_type_setting = mapping["user_addresses"][address_type]
        return AttrDict(
            settings=settings,
            address_type_uuid=address_type_setting[0],
            address_type_visibility=address_type_setting[1],
        )

    def _get_expected_mo_api_calls(self, setup, expected, mo_values, ad_data, today):
        expected_sync = {
            "noop": [],
            "create": [
                {
                    "force": True,
                    "payload": {
                        "address_type": {"uuid": setup.address_type_uuid},
                        "org": {"uuid": "org_uuid"},
                        "person": {"uuid": mo_values["uuid"]},
                        "type": "address",
                        "validity": {"from": today, "to": None},
                        "value": ad_data,
                    },
                    "url": "details/create",
                }
            ],
            "edit": [
                {
                    "force": True,
                    "payload": [
                        {
                            "data": {
                                "address_type": {"uuid": setup.address_type_uuid},
                                "validity": {"from": today, "to": None},
                                "value": ad_data,
                            },
                            "type": "address",
                            "uuid": "address_uuid",
                        }
                    ],
                    "url": "details/edit",
                }
            ],
            "terminate": [
                {
                    "force": True,
                    "payload": {
                        "type": "address",
                        "uuid": "address_uuid",
                        "validity": {"to": today},
                    },
                    "url": "details/terminate",
                }
            ],
        }
        return expected_sync[expected]

    def _get_expected_mo_api_calls_with_visibility(
        self,
        setup: AttrDict,
        expected: str,
        mo_values: dict,
        ad_data: str,
        today: str,
    ):
        calls = self._get_expected_mo_api_calls(
            setup, expected, mo_values, ad_data, today
        )

        # Enrich expected with visibility
        address_type_visibility: str = setup.address_type_visibility  # type: ignore
        if address_type_visibility:
            # Where to write visibility information
            payload_table: Dict[str, Callable] = {
                "noop": lambda: {},  # aka. throw it away
                "create": lambda: calls[0]["payload"],
                "edit": lambda: payload_table["create"]()[0]["data"],
                "terminate": lambda: {},
            }
            # Write the visibility into the table
            visibility_lower = address_type_visibility.lower()
            payload_table[expected]()["visibility"] = {
                "uuid": "address_visibility_" + visibility_lower + "_uuid"
            }

        return calls

    def _get_expected_mo_engagement_edit_call(self, validity_from=None, **data):
        call = {
            "force": True,
            "payload": {
                "data": {
                    "validity": {"from": validity_from, "to": None},
                },
                "type": "engagement",
                "uuid": "engagement_uuid",
            },
            "url": "details/edit",
        }
        call["payload"]["data"].update(**data)
        return call

    @parameterized.expand(
        [
            # Email (Undefined)
            # ------------------
            # No email in MO
            ("email", None, None, "noop"),
            ("email", "emil@magenta.dk", None, "create"),
            ("email", "example@example.com", None, "create"),
            ("email", "lee@magenta.dk", None, "create"),
            # Email already in MO
            ("email", "emil@magenta.dk", "emil@magenta.dk", "noop"),
            ("email", "example@example.com", "emil@magenta.dk", "edit"),
            ("email", "lee@magenta.dk", "emil@magenta.dk", "edit"),
            # Email terminated in AD
            ("email", None, "old.mo.email@example.org", "terminate"),
            # Telephone number (PUBLIC)
            # --------------------------
            # No telephone number in MO
            ("telephone", None, None, "noop"),
            ("telephone", "+45 70 10 11 55", None, "create"),
            ("telephone", "70 10 11 55", None, "create"),
            ("telephone", "70101155", None, "create"),
            # Telephone number already in MO
            ("telephone", "70101155", "70101155", "noop"),
            ("telephone", "90909090", "70101155", "edit"),
            ("telephone", "90901111", "70101155", "edit"),
            # Telephone number terminated in AD
            ("telephone", None, "12345678", "terminate"),
            # Office number (INTERNAL)
            # -------------------------
            # No office number in MO
            ("office", None, None, "noop"),
            ("office", "420", None, "create"),
            ("office", "421", None, "create"),
            ("office", "11", None, "create"),
            # Office number already in MO
            ("office", "420", "420", "noop"),
            ("office", "421", "420", "edit"),
            ("office", "11", "420", "edit"),
            # Office number terminated in AD
            ("office", None, "42", "terminate"),
            # Mobile number (SECRET)
            # -----------------------
            # No mobile number in MO
            ("mobile", None, None, "noop"),
            ("mobile", "+45 70 10 11 55", None, "create"),
            ("mobile", "70 10 11 55", None, "create"),
            ("mobile", "70101155", None, "create"),
            # Mobile number already in MO
            ("mobile", "70101155", "70101155", "noop"),
            ("mobile", "90909090", "70101155", "edit"),
            ("mobile", "90901111", "70101155", "edit"),
            # Mobile number terminated in AD
            ("mobile", None, "12345678", "terminate"),
            # Floor (no uuid)
            # ----------------
            # No floor number in MO
            ("floor", None, None, "noop"),
            ("floor", "1st", None, "create"),
            ("floor", "2nd", None, "create"),
            ("floor", "3rd", None, "create"),
            # Floor number already in MO
            ("floor", "1st", "1st", "noop"),
            ("floor", "2nd", "1st", "edit"),
            ("floor", "3rd", "1st", "edit"),
            # Floor number terminated in AD
            ("floor", None, "1st", "terminate"),
        ]
    )
    def test_sync_address_data(self, address_type, ad_data, mo_data, expected):
        """Verify address data is synced correctly from AD to MO.

        Args:
            address_type (str): The type of address to operate on.
            ad_data (str): The address data found in AD (if any).
            mo_data (str): The address data found in MO (if any).
            expected (str): The expected outcome of running AD sync.
                One of:
                    'noop': Nothing in MO is updated.
                    'create': A new address is created in MO.
                    'edit': The current address in MO is updated.
                    'terminate': The current address in MO is terminated.
        """
        setup = self._setup_address_type_mappings(address_type)
        today = today_iso()
        mo_values = self.mo_values_func()

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values[address_type] = ad_data
            return ad_values

        def seed_mo():
            if mo_data is None:
                return {"address": []}
            return {
                "address": [
                    {
                        "uuid": "address_uuid",
                        "address_type": {"uuid": setup.address_type_uuid},
                        "org": {"uuid": "org_uuid"},
                        "person": {"uuid": mo_values["uuid"]},
                        "type": "address",
                        "validity": {"from": today, "to": None},
                        "value": mo_data,
                    }
                ]
            }

        self._setup_admosync(
            transform_settings=lambda _: setup.settings,
            transform_ad_values=add_ad_data,
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Check that the expected MO calls were made
        self.assertEqual(
            self.ad_sync.mo_post_calls,
            self._get_expected_mo_api_calls_with_visibility(
                setup, expected, mo_values, ad_data, today
            ),
        )

    def test_sync_address_data_multiple_of_same_type(self):
        address_type = "email"
        setup = self._setup_address_type_mappings(address_type)
        today = today_iso()
        mo_values = self.mo_values_func()

        mo_value = "old value"
        ad_value = "new value"

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values[address_type] = ad_value
            return ad_values

        def seed_mo():
            three_identical_mo_addresses = [
                {
                    "uuid": "address_uuid",
                    "address_type": {"uuid": setup.address_type_uuid},
                    "org": {"uuid": "org_uuid"},
                    "person": {"uuid": mo_values["uuid"]},
                    "type": "address",
                    "validity": {"from": today, "to": None},
                    "value": mo_value,
                }
            ] * 3
            return {"address": three_identical_mo_addresses}

        self._setup_admosync(
            transform_settings=lambda _: setup.settings,
            transform_ad_values=add_ad_data,
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Check that the expected MO calls were made.
        # Assert that we make 1 'edit' call and 2 'terminate' calls, in order
        # to keep only one of the three identical MO addresses.
        expected_calls = chain(
            *[
                self._get_expected_mo_api_calls(setup, verb, mo_values, ad_value, today)
                for verb in ("edit", "terminate", "terminate")
            ]
        )
        self.assertEqual(self.ad_sync.mo_post_calls, list(expected_calls))

    def test_sync_address_data_multiple(self):
        """Verify address data is synced correctly from AD to MO."""
        today = today_iso()
        mo_values = self.mo_values_func()

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values["email"] = "emil@magenta.dk"
            ad_values["telephone"] = "70101155"
            ad_values["office"] = "11"
            return ad_values

        def seed_mo():
            return {
                "address": [
                    {
                        "uuid": "address_uuid",
                        "address_type": {"uuid": "office_uuid"},
                        "org": {"uuid": "org_uuid"},
                        "person": {"uuid": mo_values["uuid"]},
                        "type": "address",
                        "validity": {"from": today, "to": None},
                        "value": "42",
                    }
                ]
            }

        self._setup_admosync(
            transform_settings=self._sync_address_mapping_transformer(),
            transform_ad_values=add_ad_data,
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Expected outcome
        expected_sync = [
            {
                "force": True,
                "payload": {
                    "address_type": {"uuid": "email_uuid"},
                    "org": {"uuid": "org_uuid"},
                    "person": {"uuid": mo_values["uuid"]},
                    "type": "address",
                    "validity": {"from": today, "to": None},
                    "value": "emil@magenta.dk",
                },
                "url": "details/create",
            },
            {
                "force": True,
                "payload": {
                    "address_type": {"uuid": "telephone_uuid"},
                    "org": {"uuid": "org_uuid"},
                    "person": {"uuid": mo_values["uuid"]},
                    "type": "address",
                    "validity": {"from": today, "to": None},
                    "value": "70101155",
                    "visibility": {"uuid": "address_visibility_public_uuid"},
                },
                "url": "details/create",
            },
            {
                "force": True,
                "payload": [
                    {
                        "data": {
                            "address_type": {"uuid": "office_uuid"},
                            "validity": {"from": today, "to": None},
                            "value": "11",
                            "visibility": {"uuid": "address_visibility_internal_uuid"},
                        },
                        "type": "address",
                        "uuid": "address_uuid",
                    }
                ],
                "url": "details/edit",
            },
        ]
        self.assertEqual(len(self.ad_sync.mo_post_calls), 3)
        for expected_block in expected_sync:
            self.assertIn(expected_block, self.ad_sync.mo_post_calls)

    def _sync_itsystem_mapping_transformer(self):
        def add_sync_mapping(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {
                "it_systems": {"samAccountName": "it_system_uuid"}
            }
            return settings

        return add_sync_mapping

    @parameterized.expand(
        [
            ("", "djjohn", "create"),
            ("", "dijon", "create"),
            ("djjohn", "djjohn", "noop"),
            ("djjohn", "dijon", "edit"),
            ("anything_else", "djjohn", "edit"),
            ("anything_else", "dijon", "edit"),
        ]
    )
    def test_sync_itsystem(self, mo_username, ad_username, expected):
        """Verify itsystem data is synced correctly from AD to MO."""
        today = today_iso()
        mo_values = self.mo_values_func()

        def seed_mo():
            from_date = "1970-01-01"
            to_date = "9999-12-31"
            return {
                "it": [
                    {
                        "itsystem": {
                            "name": "Active Directory",
                            "uuid": "it_system_uuid",
                        },
                        "user_key": mo_username,
                        "uuid": "it_system_uuid",
                        "validity": {"from": from_date, "to": to_date},
                    }
                ]
                if mo_username != ""
                else []
            }

        def set_ad_name(ad_values):
            ad_values["SamAccountName"] = ad_username
            return ad_values

        self._setup_admosync(
            transform_settings=self._sync_itsystem_mapping_transformer(),
            transform_ad_values=set_ad_name,
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Expected outcome
        expected_sync = {
            "noop": [],
            "edit": [
                {
                    "force": True,
                    "payload": {
                        "type": "it",
                        "data": {
                            "user_key": ad_username,
                            "validity": {"from": today, "to": None},
                        },
                        "uuid": "it_system_uuid",
                    },
                    "url": "details/edit",
                }
            ],
            "create": [
                {
                    "force": True,
                    "payload": {
                        "type": "it",
                        "user_key": ad_username,
                        "itsystem": {"uuid": "it_system_uuid"},
                        "person": {"uuid": mo_values["uuid"]},
                        "validity": {"from": today, "to": None},
                    },
                    "url": "details/create",
                }
            ],
        }
        self.assertEqual(self.ad_sync.mo_post_calls, expected_sync[expected])

    def _sync_engagement_mapping_transformer(self, mo_to_ad):
        def add_sync_mapping(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {
                "engagements": mo_to_ad
            }
            return settings

        return add_sync_mapping

    @parameterized.expand(
        [
            # Move GivenName into extensions_1
            (None, ("GivenName", "extension_1"), "noop"),
            ({"extension_1": None}, ("GivenName", "extension_1"), "edit"),
            ({"extension_1": "OldValue"}, ("GivenName", "extension_1"), "edit"),
            # Move GivenName into extensions_2
            (None, ("GivenName", "extension_2"), "noop"),
            ({"extension_2": None}, ("GivenName", "extension_2"), "edit"),
            ({"extension_2": "OldValue"}, ("GivenName", "extension_2"), "edit"),
            # Move SamAccountName into extensions_1
            (None, ("SamAccountName", "extension_1"), "noop"),
            ({"extension_1": None}, ("SamAccountName", "extension_1"), "edit"),
            ({"extension_1": "OldValue"}, ("SamAccountName", "extension_1"), "edit"),
        ]
    )
    def test_sync_engagement(self, do_seed, mo_to_ad, expected):
        """Verify engagement data is synced correctly from AD to MO."""
        today = today_iso()
        ad_values = self.ad_values_func()

        def seed_mo():
            if not do_seed:
                return {"engagement": []}
            element = {
                "is_primary": True,
                "uuid": "engagement_uuid",
                "validity": {"from": "1960-06-29", "to": None},
            }
            element.update(do_seed)
            return {"engagement": [element]}

        self._setup_admosync(
            transform_settings=self._sync_engagement_mapping_transformer(
                {key: value for (key, value) in [mo_to_ad]}
            ),
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Expected outcome
        expected_sync = {
            "noop": [],
            "edit": [
                {
                    "force": True,
                    "payload": {
                        "data": {
                            mo_to_ad[1]: ad_values[mo_to_ad[0]],
                            "validity": {"from": today, "to": None},
                        },
                        "type": "engagement",
                        "uuid": "engagement_uuid",
                    },
                    "url": "details/edit",
                }
            ],
        }
        self.assertEqual(self.ad_sync.mo_post_calls, expected_sync[expected])

    def test_sync_engagement_dropped_field(self):
        """Verify engagement data is synced correctly from AD to MO.
        Test removal of dropped extensions in incoming AD object.

        This tests the code path in `_edit_engagement` where `self.lc` is
        None, e.g. the LoraCache is not configured and used.
        """
        today = today_iso()

        def seed_mo():
            element = {
                "is_primary": True,
                "uuid": "engagement_uuid",
                "validity": {"from": "1960-06-29", "to": None},
                "extension_2": "old mo value",
            }
            return {"engagement": [element]}

        self._setup_admosync(
            transform_settings=self._sync_engagement_mapping_transformer(
                # Map an attr which does not exist in the AD object
                {"extensionAttribute2": "extension_2"}
            ),
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Verify that we send an empty value to MO for 'extension_2'
        self.assertEqual(
            self.ad_sync.mo_post_calls,
            [
                self._get_expected_mo_engagement_edit_call(
                    extension_2=None, validity_from=today
                )
            ],
        )

    def test_sync_engagement_dropped_field_loracache(self):
        """Verify engagement data is synced correctly from AD to MO.
        Test removal of dropped extensions in incoming AD object.

        This test mocks the presence of a `LoraCache` instance at `self.lc`.
        """
        self._setup_admosync(
            transform_settings=self._sync_engagement_mapping_transformer(
                # Map an attr which does not exist in the AD object
                {"extensionAttribute2": "extension_2"}
            ),
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        self.ad_sync.lc = MockLoraCache(self.mo_values_func())

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Verify that we send an empty value to MO for 'extension_2'
        self.assertEqual(
            self.ad_sync.mo_post_calls,
            [
                self._get_expected_mo_engagement_edit_call(
                    extension_2=None,
                    validity_from=today_iso(),
                )
            ],
        )

    def test_sync_engagement_configuration_check_loracache(self):
        """Verify that `_edit_engagement` raises an exception if an
        unmapped MO field is encountered.

        This test mocks the presence of a `LoraCache` instance at `self.lc`.
        """
        self._setup_admosync(
            transform_settings=self._sync_engagement_mapping_transformer(
                # Map an attr which does not exist in field mapping
                {"extensionAttribute2": "unknown_field"}
            ),
        )

        self.ad_sync.lc = MockLoraCache(self.mo_values_func())

        # Run full sync against the mocks
        with self.assertRaises(Exception):
            self.ad_sync.update_all_users()

    @parameterized.expand(
        [
            (False,),  # test without LoraCache
            (True,),  # test with LoraCache
        ]
    )
    def test_sync_engagement_excludes_future_engagements(self, lora_cache):
        def seed_mo():
            current = {
                "uuid": "engagement_uuid_1",
                "extension_1": "1",
                "is_primary": True,
                "validity": {"from": "2010-12-31", "to": "2030-12-31"},
            }
            future = {
                "uuid": "engagement_uuid_2",
                "extension_1": "2",
                "is_primary": True,
                "validity": {"from": "2030-12-31", "to": None},
            }
            return {"engagement": [current, future]}

        self._setup_admosync(
            transform_settings=self._sync_engagement_mapping_transformer(
                # Map an attr whose value is not equal to the MO value
                {"ad_field": "extension_1"}
            ),
            # Mock an AD which always contains "foobar" in the field
            # "ad_field"
            transform_ad_values=dict_modifier({"ad_field": "foobar"}),
            seed_mo=seed_mo,
        )

        if lora_cache:
            self.ad_sync.lc = MockLoraCache(
                self.mo_values_func(),
                mo_engagements=seed_mo()["engagement"],
            )

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        self.assertEqual(
            self.ad_sync.mo_post_calls,
            [
                {
                    "force": True,
                    "payload": {
                        "data": {
                            "extension_1": "foobar",
                            "validity": {
                                "from": today_iso(),
                                "to": "2030-12-31",
                            },
                        },
                        "type": "engagement",
                        "uuid": "engagement_uuid_1",
                    },
                    "url": "details/edit",
                }
            ],
        )

    @parameterized.expand(
        [
            # - Finalize
            # Today
            [today_iso(), None, "terminate"],
            [today_iso(), today_iso(), "noop"],
            # 2020-01-01
            ["2020-01-01", None, "terminate"],
            ["2020-01-01", today_iso(), "noop"],
            ["2020-01-01", "2020-02-01", "noop"],  # past
            ["2020-01-01", "9999-01-01", "noop"],  # future
        ]
    )
    def test_finalization_address(self, from_date, to_date, expected):
        """Verify expected behavior from sync_disabled settings."""

        def add_sync_mapping(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {
                "user_addresses": {"email": ["email_uuid", None]}
            }
            settings["integrations.ad"][0]["ad_mo_sync_terminate_disabled"] = True
            return settings

        mo_values = self.mo_values_func()

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values["Enabled"] = False
            return ad_values

        def seed_mo():
            return {
                "address": [
                    {
                        "uuid": "address_uuid",
                        "address_type": {"uuid": "email_uuid"},
                        "org": {"uuid": "org_uuid"},
                        "person": {"uuid": mo_values["uuid"]},
                        "type": "address",
                        "validity": {"from": from_date, "to": to_date},
                        "value": "42",
                    }
                ]
            }

        self._setup_admosync(
            transform_settings=add_sync_mapping,
            transform_ad_values=add_ad_data,
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        today = today_iso()
        sync_expected = {
            "terminate": [
                {
                    "force": True,
                    "payload": {
                        "type": "address",
                        "uuid": "address_uuid",
                        "validity": {"to": today},
                    },
                    "url": "details/terminate",
                }
            ],
            "noop": [],
        }
        self.assertEqual(self.ad_sync.mo_post_calls, sync_expected[expected])

    @parameterized.expand(
        [
            # - Finalize
            # Today
            [today_iso(), None, "terminate"],
            [today_iso(), today_iso(), "noop"],
            # 2020-01-01
            ["2020-01-01", None, "terminate"],
            ["2020-01-01", today_iso(), "noop"],
            ["2020-01-01", "2020-02-01", "noop"],  # past
            ["2020-01-01", "9999-01-01", "noop"],  # future
        ]
    )
    def test_finalization_itsystem(self, from_date, to_date, expected):
        """Verify expected behavior from sync_disabled settings."""

        def add_sync_mapping(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {
                "it_systems": {"samAccountName": "it_system_uuid"}
            }
            settings["integrations.ad"][0]["ad_mo_sync_terminate_disabled"] = True
            return settings

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values["Enabled"] = False
            return ad_values

        def seed_mo():
            return {
                "it": [
                    {
                        "itsystem": {
                            "name": "Active Directory",
                            "uuid": "it_system_uuid",
                        },
                        "user_key": "username",
                        "uuid": "itconnection_uuid",
                        "validity": {"from": from_date, "to": to_date},
                    }
                ]
            }

        self._setup_admosync(
            transform_settings=add_sync_mapping,
            transform_ad_values=add_ad_data,
            seed_mo=seed_mo,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        today = today_iso()
        sync_expected = {
            "terminate": [
                {
                    "force": True,
                    "payload": {
                        "type": "it",
                        "uuid": "itconnection_uuid",
                        "validity": {"to": today},
                    },
                    "url": "details/terminate",
                }
            ],
            "noop": [],
        }
        self.assertEqual(self.ad_sync.mo_post_calls, sync_expected[expected])

    @parameterized.expand(
        [
            ([], True),
            (["True"], True),
            (["False"], False),
            (["True", "True"], True),
            (["True", "False"], False),
            (["False", "True"], False),
            (["False", "False"], False),
            (["{{ 'a' == 'a' }}"], True),
            (["{{ 'a' == 'b' }}"], False),
            (["{{ 2 == 3 }}"], False),
            (["{{ ad_object['extensionAttribute1']|length == 10 }}"], True),
        ]
    )
    def test_pre_filters(self, prefilters, expected):
        def add_prefilter_template(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {}
            settings["integrations.ad"][0]["ad_mo_sync_pre_filters"] = prefilters
            return settings

        self._setup_admosync(
            transform_settings=add_prefilter_template,
        )
        self.assertEqual(self.ad_sync.mo_post_calls, [])

        update_mock = MagicMock()
        self.ad_sync._update_single_user = update_mock

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        if expected:
            update_mock.assert_called()
        else:
            update_mock.assert_not_called()

    @parameterized.expand(
        [
            ([], False, False),
            (["True"], False, False),
            (["False"], False, False),
            ([], True, True),
            (["True"], True, True),
            (["False"], True, True),
            ([], None, False),
            (["True"], None, True),
            (["False"], None, False),
            (["True", "True"], None, True),
            (["True", "False"], None, False),
            (["False", "True"], None, False),
            (["False", "False"], None, False),
        ]
    )
    def test_disabled_filter(self, prefilters, terminate_disabled, expected):
        def add_terminate_filter_template(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {}
            settings["integrations.ad"][0][
                "ad_mo_sync_terminate_disabled"
            ] = terminate_disabled
            settings["integrations.ad"][0][
                "ad_mo_sync_terminate_disabled_filters"
            ] = prefilters
            return settings

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values["Enabled"] = False
            return ad_values

        self._setup_admosync(
            transform_settings=add_terminate_filter_template,
            transform_ad_values=add_ad_data,
        )
        self.assertEqual(self.ad_sync.mo_post_calls, [])

        finalize_mock = MagicMock()
        self.ad_sync._finalize_it_system = finalize_mock
        self.ad_sync._finalize_user_addresses = finalize_mock

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        if expected:
            finalize_mock.assert_called()
        else:
            finalize_mock.assert_not_called()

    @parameterized.expand(
        [
            (False, False),
            (True, True),
        ]
    )
    def test_finalize_missing_ad_user(self, terminate_missing, expected):
        def add_terminate_filter_template(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {}
            settings["integrations.ad"][0][
                "ad_mo_sync_terminate_missing"
            ] = terminate_missing
            settings["integrations.ad"][0][
                "ad_mo_sync_terminate_missing_require_itsystem"
            ] = False
            return settings

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            return None

        self._setup_admosync(
            transform_settings=add_terminate_filter_template,
            transform_ad_values=add_ad_data,
        )
        self.assertEqual(self.ad_sync.mo_post_calls, [])

        finalize_mock = MagicMock()
        engagement_mock = MagicMock()
        self.ad_sync._finalize_it_system = finalize_mock
        self.ad_sync._finalize_user_addresses = finalize_mock
        self.ad_sync._edit_engagement = engagement_mock

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        if expected:
            finalize_mock.assert_called()
            engagement_mock.assert_called()
        else:
            finalize_mock.assert_not_called()


class TestADMoSyncEditUserAttrs(TestCase, TestADMoSyncMixin):
    maxDiff = None
    generate_dynamic_person = False

    def setUp(self):
        self._initialize_configuration()

    @parameterized.expand(
        [
            # 0: No fields are mapped
            (
                {},  # (empty) mapping of AD attrs to MO attrs
                {},  # (additional) AD attrs for this user
            ),
            # 1: "Given name" is mapped from AD to MO, and AD value is present
            (
                {"givenName": "givenname"},
                {"givenName": "Test"},
            ),
            # 2: "Given name" is mapped from AD to MO, but AD value is not
            # present
            (
                {"givenName": "givenname"},
                {},
            ),
            # 3: Field A is mapped from AD to MO, but only field B has a
            # present AD value
            (
                {"foo": "foo"},
                {"bar": "AD value for bar"},
            ),
            # 4: Fields A and B are mapped, but only field B has a present AD
            # value
            (
                {"givenName": "givenname", "surname": "surname"},
                {"givenName": "Test"},
            ),
            # 5: Fields A and B are mapped, and both fields have a present AD
            # value
            (
                {"givenName": "givenname", "surname": "surname"},
                {"givenName": "Test", "surname": "Testesen"},
            ),
        ]
    )
    def test_edit_user_attrs(self, mapping, ad_attrs):
        def add_sync_mapping(settings):
            settings["integrations.ad"][0]["ad_mo_sync_mapping"] = {
                "user_attrs": mapping
            }
            return settings

        def add_ad_data(ad_values):
            ad_values.update(ad_attrs)
            return ad_values

        self._setup_admosync(
            transform_settings=add_sync_mapping,
            transform_ad_values=add_ad_data,
        )

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # If AD attrs are mapped and present in AD object
        if set(ad_attrs) and set(ad_attrs).issubset(set(mapping)):
            # Assert exactly one MO POST call was made
            self.assertEqual(len(self.ad_sync.mo_post_calls), 1)
            call = self.ad_sync.mo_post_calls[0]
            data = call["payload"]["data"]
            # Assert payload is "edit employee" with validity of (today, None)
            self.assertEqual(call["url"], "details/edit")
            self.assertEqual(call["payload"]["type"], "employee")
            self.assertDictEqual(data["validity"], {"from": today_iso(), "to": None})
            # Assert we use AD value for each mapped field
            for ad_attr, mo_attr in mapping.items():
                if ad_attr in ad_attrs:
                    self.assertEqual(data[mo_attr], ad_attrs[ad_attr])
                else:
                    self.assertNotIn(ad_attr, data)
        else:
            # Assert no MO POST calls were made
            self.assertEqual(len(self.ad_sync.mo_post_calls), 0)


class TestReadAllMOUsers(TestCase):
    def test_returns_users(self):
        """`AdMoSync._read_all_mo_users` must return all non-empty users found in
        `self.lc.users` if using LoraCache.

        See: #48568
        """
        instance = _TestableAdMoSyncLoraCache()
        expected_users = [MO_VALUES]
        actual_users = instance._read_all_mo_users()
        self.assertEqual(actual_users, expected_users)

    def test_handles_empty_loracache_user(self):
        """Regression test. `AdMoSync._read_all_mo_users` did not handle "empty
        LoraCache users" well, and would fail with an `IndexError`.

        (An "empty LoraCache user" is a key/value pair where the key is a valid UUID but
        the value is an empty list.)

        See: #48568, #50410
        """
        instance = _TestableAdMoSyncLoraCacheEmptyEmployee()
        employees = instance._read_all_mo_users()
        self.assertEqual(employees, [])


class TestEditUserAttrsLoraCache:
    def test_edit_user_attrs_is_idempotent(self):
        # Invoke `_edit_user_attrs` twice on the same MO user and AD user
        instance = _TestableAdMoSyncLoraCacheUserAttrs()
        instance._update_users([instance.mo_user])
        instance._update_users([instance.mo_user])

        # Assert that we only issue *one* call to `self.helper.update_user` in
        # `_edit_user_attrs` as neither the AD user or MO user has changed between the
        # two update attempts.
        assert len(instance.helper.update_user_calls) == 1

        # Assert contents of the single update
        mo_uuid, mo_data = instance.helper.update_user_calls[0]
        ad_user = instance.mock_ad_reader.read_user()
        assert mo_uuid == MO_UUID
        assert mo_data["user_key"] == ad_user["UserPrincipalName"]
        assert mo_data["validity"] == {"from": today_iso(), "to": None}
