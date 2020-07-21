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
    def _sync_mapping_transformer(self):
        def add_sync_mapping(settings):
            settings["integrations.ad.ad_mo_sync_mapping"] = {
                "user_addresses": {
                    "mail": ["mail_uuid", None],
                    "telephone": ["telephone_uuid", "PUBLIC"],
                    "office": ["", 'INTERNAL'],
                    "mobile": ["mobile_uuid", "SECRET"]
                },
            }
            return settings
        return add_sync_mapping


    def setUp(self):
        self._setup_admosync(
            transform_settings=self._sync_mapping_transformer(),
        )

    @parameterized.expand([
        # No email in MO
        (None, None, 'noop'),
        ('emil@magenta.dk', None, 'create'),
        ('example@example.com', None, 'create'),
        ('lee@magenta.dk', None, 'create'),
        # Email already in MO
        ('emil@magenta.dk', 'emil@magenta.dk', 'noop'),
        ('example@example.com', 'emil@magenta.dk', 'edit'),
        ('lee@magenta.dk', 'emil@magenta.dk', 'edit'),
    ])
    def test_sync_email(self, ad_email, mo_email, expected):
        """Verify email addresses are synced correctly from AD to MO.

        Args:
            ad_email (str): The email address found in AD (if any).
            mo_email (str): The email address found in MO (if any).
            expected (str): The expected outcome of running AD sync.
                One of:
                    'noop': Nothing in MO is updated.
                    'create': A new email address is created in MO.
                    'edit': The current email address in MO is updated.
        """

        today = date.today().strftime("%Y-%m-%d")
        mo_values = self.mo_values_func()

        # Helper functions to seed admosync mock
        def add_ad_mail(ad_values):
            ad_values['mail'] = ad_email
            return ad_values

        def seed_mo_addresses():
            if mo_email is None:
                return []
            return [{
                'uuid': 'address_uuid',
                'address_type': {'uuid': 'mail_uuid'},
                'org': {'uuid': 'org_uuid'},
                'person': {'uuid': mo_values['uuid']},
                'type': 'address',
                'validity': {'from': today, 'to': None},
                'value': mo_email,
            }]

        self._setup_admosync(
            transform_settings=self._sync_mapping_transformer(),
            transform_ad_values=add_ad_mail,
            seed_mo_addresses=seed_mo_addresses,
        )

        self.assertEqual(self.ad_sync.mo_post_calls, [])

        # Run full sync against the mocks
        self.ad_sync.update_all_users()

        # Expected outcome
        expected_sync = {
            'noop': [],
            'create': [
                {
                    'force': True,
                    'payload': {
                        'address_type': {'uuid': 'mail_uuid'},
                        'org': {'uuid': 'org_uuid'},
                        'person': {'uuid': mo_values['uuid']},
                        'type': 'address',
                        'validity': {'from': today, 'to': None},
                        'value': ad_email,
                    },
                    'url': 'details/create'
                },
            ],
            'edit': [
                {
                    'force': True,
                    'payload': [
                        {
                            'data': {
                                'address_type': {'uuid': 'mail_uuid'},
                                'validity': {'from': today, 'to': None},
                                'value': ad_email
                            },
                            'type': 'address',
                            'uuid': 'address_uuid'
                        }
                    ],
                    'url': 'details/edit'
                }
            ]
        }
        self.assertEqual(self.ad_sync.mo_post_calls, expected_sync[expected])
