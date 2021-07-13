from unittest import mock
from unittest import TestCase

from os2mo_helpers.mora_helpers import MoraHelper

from .. import ad_common
from .. import sync_mo_uuid_to_ad
from ..ad_exceptions import ImproperlyConfigured
from .mocks import MockADParameterReader


_MO_UUID = "not-a-uuid"
_AD_UUID_FIELD = "uuidField"


class _SyncMoUuidToAd(sync_mo_uuid_to_ad.SyncMoUuidToAd):
    def _create_session(self):
        return mock.MagicMock()

    def _search_mo_cpr(self, cpr):
        return _MO_UUID

    def _build_user_credential(self):
        return ""

    def _run_ps_script(self, ps_script):
        self._scripts = getattr(self, "_scripts", [])
        self._scripts.append(ps_script)


class TestSyncMoUuidToAd(TestCase):
    """Test `sync_mo_uuid_to_ad`"""

    def test_invalid_configuration(self):
        with self.assertRaises(ImproperlyConfigured):
            self._get_instance(
                {
                    "integrations.ad.write.uuid_field": "foo",
                    "integrations.ad": [{"properties": ["bar", "baz"]}],
                }
            )

    def test_configure_mora_helper(self):
        instance = self._get_instance()
        self.assertIsInstance(instance.helper, MoraHelper)
        self.assertIsInstance(instance.org_uuid, str)

    def test_sync_one(self):
        instance = self._get_instance()
        ad_cpr_no = instance.reader.read_user()["extensionAttribute1"]
        instance.sync_one(ad_cpr_no)
        # Assert we tried to execute one script with the proper contents
        self.assertEqual(len(instance._scripts), 1)
        self.assertIn(
            '-Replace @{"%s"="%s"}' % (_AD_UUID_FIELD, _MO_UUID),
            instance._scripts[0],
        )

    def _get_instance(self, settings=None):
        _settings = {
            "integrations.ad.write.uuid_field": _AD_UUID_FIELD,
            "integrations.ad": [{"properties": [_AD_UUID_FIELD]}],
            "global": {"mora.base": None},
            "primary": {"cpr_field": "extensionAttribute1", "cpr_separator": "-"},
        }

        if settings:
            _settings.update(settings)

        with mock.patch.object(ad_common, "read_settings", return_value=_settings):
            with mock.patch.object(
                sync_mo_uuid_to_ad, "load_settings", return_value=_settings
            ):
                with mock.patch.object(
                    sync_mo_uuid_to_ad,
                    "MoraHelper",
                    spec=MoraHelper,
                    read_organisation=lambda: "123",
                ):
                    with mock.patch.object(
                        sync_mo_uuid_to_ad,
                        "ADParameterReader",
                        new=MockADParameterReader,
                    ):
                        return _SyncMoUuidToAd()
