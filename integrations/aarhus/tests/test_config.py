from uuid import uuid4

import config
import pytest
import uuids

from .helpers import HelperMixin


class TestConfig(HelperMixin):
    def test_get_config_fails_if_unconfigured(self):
        with pytest.raises(config.ImproperlyConfigured):
            config.get_config()

    def test_get_config_succeeds_if_configured(self):
        with self._mock_settings_json():
            settings_a = config.Settings.from_kwargs(**self._get_config_kwargs())
            settings_b = config.get_config()
            assert settings_a == settings_b

    def test_settings_include_default(self):
        with self._mock_settings_json():
            settings = config.Settings.from_kwargs(**self._get_config_kwargs())
            assert settings.azid_it_system_uuid == uuids.AZID_SYSTEM

    def test_settings_include_json(self):
        azid_uuid = uuid4()
        settings = {"integrations.aarhus_los.azid_it_system_uuid": str(azid_uuid)}
        with self._mock_settings_json(settings):
            settings = config.Settings.from_kwargs(**self._get_config_kwargs())
            assert settings.azid_it_system_uuid == azid_uuid

    def _get_config_kwargs(self):
        # Provide values for all settings that do not have a default value
        return dict(
            ftp_url="ftp-url",
            ftp_user="ftp-user",
            ftp_pass="ftp-pass",
            ftp_folder="ftp-folder",
            import_state_file="import-state-file",
        )
