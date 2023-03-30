from contextlib import contextmanager
from logging import ERROR
from unittest import mock
from unittest import TestCase

import pytest
from parameterized import parameterized

from .. import ad_common
from .. import sync_mo_uuid_to_ad
from ..ad_exceptions import ImproperlyConfigured
from .mocks import AD_UUID_FIELD
from .mocks import MO_UUID
from .mocks import MockADParameterReader
from .mocks import MockMoraHelper
from .mocks import MockUnknownCPRADParameterReader
from .mocks import UNKNOWN_CPR_NO


class _SyncMoUuidToAd(sync_mo_uuid_to_ad.SyncMoUuidToAd):
    """Testable subclass of the real `SyncMoUuidToAd` class."""

    def __init__(self, ad_cpr_no):
        self._ad_cpr_no = ad_cpr_no
        self._scripts = []
        super().__init__()

    def _get_mora_helper(self):
        return MockMoraHelper(self._ad_cpr_no)

    def _create_session(self):
        return mock.MagicMock()

    def _build_user_credential(self):
        return ""

    def _run_ps_script(self, ps_script):
        self._scripts.append(ps_script)


class _SyncMoUuidToAdRunPSScriptFails(_SyncMoUuidToAd):
    def _run_ps_script(self, ps_script):
        super()._run_ps_script(ps_script)
        raise Exception("an exception!")


# Based on this example:
# https://docs.pytest.org/en/stable/example/parametrize.html#parametrizing-conditional-raising


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "example_input,expectation",
    [
        ([{"properties": ["foo"]}], does_not_raise()),
        ([{"properties": ["bar", "baz"]}], pytest.raises(ImproperlyConfigured)),
        (
            [{"properties": ["bar", "baz"]}, {"properties": ["bar", "baz"]}],
            pytest.raises(ImproperlyConfigured),
        ),
        (
            [{"properties": ["foo", "baz"]}, {"properties": ["bar", "baz"]}],
            does_not_raise(),
        ),
    ],
)
def test_invalid_configuration(example_input, expectation):
    with expectation:
        TestSyncMoUuidToAd()._get_instance(
            {
                "integrations.ad.write.uuid_field": "foo",
                "integrations.ad": example_input,
            }
        )


class TestSyncMoUuidToAd(TestCase):
    """Test `sync_mo_uuid_to_ad`"""

    _ad_cpr_field_name = "extensionAttribute1"
    _mo_cpr_no = "0123456789"

    def test_sync_one(self):
        instance = self._get_instance()
        instance.sync_one(instance._ad_cpr_no)
        self._assert_script_contents_ok(instance)

    @parameterized.expand(
        [
            # Look up bogus CPR in mock AD which will not find an AD user
            (MockUnknownCPRADParameterReader(), "AD User not found"),
            # Look up bogus CPR in mock MO which will not find a MO user
            (MockADParameterReader(), "MO User not found"),
        ]
    )
    def test_sync_one_unknown_cpr_raises_exception(self, reader, expected_message):
        instance = self._get_instance(reader=reader)
        with self.assertRaisesRegex(Exception, expected_message):
            instance.sync_one(UNKNOWN_CPR_NO)

    def test_sync_one_uses_morahelper(self):
        instance = self._get_instance()
        instance.sync_one(self._mo_cpr_no)
        # Assert that our mocked `MoraHelper` recorded one call to `read_user`
        # with the expected CPR as its only argument.
        self.assertListEqual(instance.helper._read_user_calls, [self._mo_cpr_no])

    def test_sync_all(self):
        instance = self._get_instance()
        instance.sync_all()
        self._assert_script_contents_ok(instance)

    def test_perform_sync_handles_run_ps_script_exception(self):
        ad_users = [
            {
                self._ad_cpr_field_name: self._mo_cpr_no,
                "SamAccountName": "example",
            }
        ]
        mo_users = {self._mo_cpr_no: MO_UUID}
        instance = self._get_instance(cls=_SyncMoUuidToAdRunPSScriptFails)
        with self.assertLogs("MoUuidAdSync", ERROR) as cm:
            instance.perform_sync(ad_users, mo_users)
            self.assertIn("failed to write MO UUID", cm.records[0].message)

    def _get_instance(self, settings=None, reader=None, cls=_SyncMoUuidToAd):
        _settings = {
            "integrations.ad.write.uuid_field": AD_UUID_FIELD,
            "integrations.ad": [{"properties": [AD_UUID_FIELD]}],
            "primary": {"cpr_field": self._ad_cpr_field_name, "cpr_separator": "-"},
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

        reader = reader or MockADParameterReader()

        reader_mock = mock.patch.object(
            sync_mo_uuid_to_ad,
            "ADParameterReader",
            new=lambda: reader,
        )

        ad_cpr_no = reader.read_user()[self._ad_cpr_field_name]

        with read_settings_mock:
            with load_settings_mock:
                with reader_mock:
                    instance = cls(ad_cpr_no)
                    return instance

    def _assert_script_contents_ok(self, instance):
        # Assert we tried to execute exactly one script, with the proper
        # contents.
        self.assertEqual(len(instance._scripts), 1)
        self.assertIn(
            '-Replace @{"%s"="%s"}' % (AD_UUID_FIELD, MO_UUID),
            instance._scripts[0],
        )
