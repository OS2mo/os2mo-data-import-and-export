from unittest import TestCase
from uuid import uuid4

from ..ad_sync import AdMoSync
from .mocks import MockLoraCache
from .mocks import MockLoraCacheEmptyEmployee
from .mocks import MockMoraHelper


class _TestableAdMoSync(AdMoSync):
    def __init__(self, mock_lora_cache):
        self._mock_lora_cache = mock_lora_cache
        super().__init__(all_settings={})

    def _setup_mora_helper(self):
        return MockMoraHelper("not-a-cpr")

    def _setup_lora_cache(self):
        return self._mock_lora_cache

    def _setup_visibilities(self):
        pass


_mo_values = {"uuid": uuid4()}


class TestAdMoSyncRegressions(TestCase):
    def test_read_all_mo_users_returns_users(self):
        instance = _TestableAdMoSync(MockLoraCache(_mo_values))
        expected_users = [_mo_values]
        actual_users = instance._read_all_mo_users()
        self.assertEqual(actual_users, expected_users)

    def test_read_all_mo_users_crashes_on_empty_user(self):
        """`AdMoSync._read_all_mo_users` raises an `IndexError` if it
        encounters one or more "empty" users in `self.lc.users`.
        (An "empty" user is a key/value pair where the key is a valid UUID but
        the value is an empty list.)
        See #48568.
        """
        instance = _TestableAdMoSync(MockLoraCacheEmptyEmployee(_mo_values))
        with self.assertRaises(IndexError):
            instance._read_all_mo_users()
