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
                    "telephoneNumber": ["telephone_uuid", "PUBLIC"],
                    "mail": ["mail_uuid", None],
                    "physicalDeliveryOfficeName": ["", 'INTERNAL'],
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
        ('emil@magenta.dk',),
        ('example@example.com',),
        ('lee@magenta.dk',),
    ])
    def test_sync_create_email(self, email):
        def add_ad_mail(ad_values):
            ad_values['mail'] = email
            return ad_values

        self._setup_admosync(
            transform_settings=self._sync_mapping_transformer(),
            transform_ad_values=add_ad_mail,
        )


        self.assertEqual(self.ad_sync.mo_post_calls, [])

        self.ad_sync.update_all_users()

        mo_values = self.mo_values_func()
        today = date.today().strftime("%Y-%m-%d")

        expected_sync = [
            {
                'force': True,
                'payload': {
                    'address_type': {'uuid': 'mail_uuid'},
                    'org': {'uuid': 'org_uuid'},
                    'person': {'uuid': mo_values['uuid']},
                    'type': 'address',
                    'validity': {'from': today, 'to': None},
                    'value': email,
                },
                'url': 'details/create'
            },
        ]
        self.assertEqual(self.ad_sync.mo_post_calls, expected_sync)
