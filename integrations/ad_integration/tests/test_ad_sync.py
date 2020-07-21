# TODO: Fix imports in module
import sys
from datetime import date
from os.path import dirname

sys.path.append(dirname(__file__))
sys.path.append(dirname(__file__) + "/..")

from unittest import TestCase

from parameterized import parameterized
from test_utils import TestADMoSyncMixin, dict_modifier, mo_modifier


class TestADMoSync(TestCase, TestADMoSyncMixin):
    def setUp(self):
        self._initialize_configuration()

    def _sync_address_mapping_transformer(self):
        def add_sync_mapping(settings):
            settings["integrations.ad.ad_mo_sync_mapping"] = {
                "user_addresses": {
                    # Different visibility
                    "email": ["email_uuid", None],
                    "telephone": ["telephone_uuid", "PUBLIC"],
                    "office": ["office_uuid", "INTERNAL"],
                    "mobile": ["mobile_uuid", "SECRET"],
                    # No uuid
                    "floor": ["", None],
                },
            }
            return settings

        return add_sync_mapping

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
        """
        today = date.today().strftime("%Y-%m-%d")
        mo_values = self.mo_values_func()
        self.settings = self._prepare_settings(
            self._sync_address_mapping_transformer()
        )
        address_type_setting = self.settings["integrations.ad.ad_mo_sync_mapping"][
            "user_addresses"
        ][address_type]
        address_type_uuid = address_type_setting[0]
        address_type_visibility = address_type_setting[1]

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values[address_type] = ad_data
            return ad_values

        def seed_mo_addresses():
            if mo_data is None:
                return []
            return [
                {
                    "uuid": "address_uuid",
                    "address_type": {"uuid": address_type_uuid},
                    "org": {"uuid": "org_uuid"},
                    "person": {"uuid": mo_values["uuid"]},
                    "type": "address",
                    "validity": {"from": today, "to": None},
                    "value": mo_data,
                }
            ]

        self._setup_admosync(
            transform_settings=lambda _: self.settings,
            transform_ad_values=add_ad_data,
            seed_mo_addresses=seed_mo_addresses,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Expected outcome
        expected_sync = {
            "noop": [],
            "create": [
                {
                    "force": True,
                    "payload": {
                        "address_type": {"uuid": address_type_uuid},
                        "org": {"uuid": "org_uuid"},
                        "person": {"uuid": mo_values["uuid"]},
                        "type": "address",
                        "validity": {"from": today, "to": None},
                        "value": ad_data,
                    },
                    "url": "details/create",
                },
            ],
            "edit": [
                {
                    "force": True,
                    "payload": [
                        {
                            "data": {
                                "address_type": {"uuid": address_type_uuid},
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
        }
        # Enrich expected with visibility
        if address_type_visibility:
            # Where to write visibility information
            payload_table = {
                "noop": lambda: {},  # aka. throw it away
                "create": lambda: expected_sync[expected][0]["payload"],
                "edit": lambda: payload_table["create"]()[0]["data"],
            }
            # Write the visibility into the table
            visibility_lower = address_type_visibility.lower()
            payload_table[expected]()["visibility"] = {
                "uuid": "address_visibility_" + visibility_lower + "_uuid"
            }

        self.assertEqual(self.ad_sync.mo_post_calls, expected_sync[expected])

    def test_sync_address_data_multiple(self):
        """Verify address data is synced correctly from AD to MO."""
        today = date.today().strftime("%Y-%m-%d")
        mo_values = self.mo_values_func()

        # Helper functions to seed admosync mock
        def add_ad_data(ad_values):
            ad_values["email"] = "emil@magenta.dk"
            ad_values["telephone"] = "70101155"
            ad_values["office"] = "11"
            return ad_values

        def seed_mo_addresses():
            return [
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

        self._setup_admosync(
            transform_settings=self._sync_address_mapping_transformer(),
            transform_ad_values=add_ad_data,
            seed_mo_addresses=seed_mo_addresses,
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
                            "visibility": {
                                "uuid": "address_visibility_internal_uuid"
                            },
                        },
                        "type": "address",
                        "uuid": "address_uuid",
                    },
                ],
                "url": "details/edit",
            },
        ]
        self.assertEqual(len(self.ad_sync.mo_post_calls), 3)
        self.assertEqual(self.ad_sync.mo_post_calls, expected_sync)

    def _sync_itsystem_mapping_transformer(self):
        def add_sync_mapping(settings):
            settings["integrations.ad.ad_mo_sync_mapping"] = {
                "it_systems": {"samAccountName": "it_system_uuid"}
            }
            return settings

        return add_sync_mapping

    @parameterized.expand(
        [("", "create",), ("username_found", "noop",), ("anything_else", "noop",),]
    )
    def test_sync_itsystem(self, e_username, expected):
        """Verify itsystem data is synced correctly from AD to MO."""
        today = date.today().strftime("%Y-%m-%d")
        ad_values = self.ad_values_func()
        mo_values = self.mo_values_func()

        def get_e_username():
            return e_username

        self._setup_admosync(
            transform_settings=self._sync_itsystem_mapping_transformer(),
            seed_e_username=get_e_username,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Expected outcome
        expected_sync = {
            "noop": [],
            "create": [
                {
                    "force": True,
                    "payload": {
                        "type": "it",
                        "user_key": ad_values["SamAccountName"],
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
            settings["integrations.ad.ad_mo_sync_mapping"] = {
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
        today = date.today().strftime("%Y-%m-%d")
        ad_values = self.ad_values_func()
        mo_values = self.mo_values_func()

        def seed_engagements():
            if not do_seed:
                return []
            element = {
                "is_primary": True,
                "uuid": "engagement_uuid",
                "validity": {"from": "1960-06-29", "to": None},
            }
            element.update(do_seed)
            return [element]

        self._setup_admosync(
            transform_settings=self._sync_engagement_mapping_transformer(
                {key: value for (key, value) in [mo_to_ad]}
            ),
            seed_engagements=seed_engagements,
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
