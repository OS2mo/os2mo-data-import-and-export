from unittest import TestCase
from uuid import uuid4

from parameterized import parameterized

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
    @parameterized.expand(
        [
            (MockLoraCache, [_mo_values]),
            (MockLoraCacheEmptyEmployee, []),
        ]
    )
    def test_read_all_mo_users_handles_empty_user(
        self, mock_lora_cache_class, expected_users
    ):
        """Regression test for #48568: `AdMoSync._read_all_mo_users` raised an
        `IndexError` if it encountered one or more "empty" users in
        `self.lc.users`.
        (An "empty" user is a key/value pair where the key is a valid UUID but
        the value is an empty list.)
        """
        instance = _TestableAdMoSync(mock_lora_cache_class(_mo_values))
        actual_users = instance._read_all_mo_users()
        self.assertEqual(actual_users, expected_users)
