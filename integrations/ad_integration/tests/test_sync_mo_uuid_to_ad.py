from unittest import mock
from unittest import TestCase

from os2mo_helpers.mora_helpers import MoraHelper

from .. import ad_common
from .. import sync_mo_uuid_to_ad
from ..ad_exceptions import ImproperlyConfigured
from .mocks import MockADParameterReader


_MO_UUID = "not-a-uuid"
_AD_UUID_FIELD = "uuidField"


class _MockMoraHelper(MoraHelper):
    def __init__(self, cpr):
        self._mo_user = {"cpr_no": cpr, "uuid": _MO_UUID}
        super().__init__()

    def read_organisation(self):
        return "not-a-org-uuid"

    def read_all_users(self):
        return [self._mo_user]


class _SyncMoUuidToAd(sync_mo_uuid_to_ad.SyncMoUuidToAd):
    """Testable subclass of the real `SyncMoUuidToAd` class."""

    def __init__(self, ad_cpr_no):
        self._ad_cpr_no = ad_cpr_no
        self._scripts = []
        super().__init__()

    def _get_mora_helper(self):
        return _MockMoraHelper(self._ad_cpr_no)

    def _search_mo_cpr(self, cpr):
        return _MO_UUID

    def _create_session(self):
        return mock.MagicMock()

    def _build_user_credential(self):
        return ""

    def _run_ps_script(self, ps_script):
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

    def test_sync_one(self):
        instance = self._get_instance()
        instance.sync_one(instance._ad_cpr_no)
        self._assert_script_contents_ok(instance)

    def test_sync_all(self):
        instance = self._get_instance()
        instance.sync_all()
        self._assert_script_contents_ok(instance)

    def _get_instance(self, settings=None):
        _settings = {
            "integrations.ad.write.uuid_field": _AD_UUID_FIELD,
            "integrations.ad": [{"properties": [_AD_UUID_FIELD]}],
            "primary": {"cpr_field": "extensionAttribute1", "cpr_separator": "-"},
            "global": {},
        }

        if settings:
            _settings.update(settings)

        read_settings_mock = mock.patch.object(
            ad_common,
            "read_settings",
            return_value=_settings,
        )

        load_settings_mock = mock.patch.object(
            sync_mo_uuid_to_ad,
            "load_settings",
            return_value=_settings,
        )

        reader = MockADParameterReader()

        reader_mock = mock.patch.object(
            sync_mo_uuid_to_ad,
            "ADParameterReader",
            new=lambda: reader,
        )

        with read_settings_mock:
            with load_settings_mock:
                with reader_mock:
                    instance = _SyncMoUuidToAd(
                        ad_cpr_no=reader.read_user()["extensionAttribute1"]
                    )
                    return instance

    def _assert_script_contents_ok(self, instance):
        # Assert we tried to execute exactly one script, with the proper
        # contents.
        self.assertEqual(len(instance._scripts), 1)
        self.assertIn(
            '-Replace @{"%s"="%s"}' % (_AD_UUID_FIELD, _MO_UUID),
            instance._scripts[0],
        )
