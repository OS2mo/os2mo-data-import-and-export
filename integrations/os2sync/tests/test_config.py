from unittest.mock import patch
from uuid import uuid4

import pytest
from parameterized import parameterized
from pydantic import ValidationError

from integrations.os2sync.config import get_os2sync_settings

env_municipality = "1234"
env_uuid = uuid4()
file_municipality = "5678"
file_uuid = uuid4()
file_api_url = "http://defined.in/file"


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("MUNICIPALITY", env_municipality)
    monkeypatch.setenv("OS2SYNC_TOP_UNIT_UUID", str(env_uuid))


class TestConfig:
    dummy_config = {
        "municipality.cvr": file_municipality,
        "os2sync_top_unit_uuid": file_uuid,
        "os2sync.api_url": file_api_url,
    }

    @patch("integrations.os2sync.config.load_settings", return_value={})
    def test_no_settings(self, settings_mock):
        with pytest.raises(ValidationError):
            get_os2sync_settings.cache_clear()
            get_os2sync_settings()

    @patch("integrations.os2sync.config.load_settings", return_value=dummy_config)
    def test_minimal_settings_file(self, settings_mock):
        get_os2sync_settings.cache_clear()
        settings = get_os2sync_settings()
        assert settings.municipality == self.dummy_config["municipality.cvr"]
        assert (
            settings.os2sync_top_unit_uuid == self.dummy_config["os2sync_top_unit_uuid"]
        )
        assert settings.os2sync_api_url == self.dummy_config["os2sync.api_url"]

    @patch("integrations.os2sync.config.load_settings", return_value={})
    def test_minimal_settings_env(self, mock_settings_file, mock_env):
        get_os2sync_settings.cache_clear()
        settings = get_os2sync_settings()
        assert settings.municipality == env_municipality
        assert settings.os2sync_top_unit_uuid == env_uuid

    @patch("integrations.os2sync.config.load_settings", return_value=dummy_config)
    def test_env_overrides(self, mock_settings_file, mock_env):
        get_os2sync_settings.cache_clear()
        settings = get_os2sync_settings()
        # Exists in both env and file, using env
        assert settings.municipality == env_municipality
        assert settings.os2sync_top_unit_uuid == env_uuid
        # Exists only in file
        assert settings.os2sync_api_url == file_api_url

    def test_full_config(self):
        conf = {
            "municipality.cvr": "test",
            "mora.base": "http://testos2mo.dk",
            "os2sync.top_unit_uuid": uuid4(),
            "os2sync.api_url": "http://testos2sync.dk",
            "os2sync.use_lc_db": False,
            "os2sync.log_level": 1,
            "os2sync.log_file": "test.log",
            "os2sync.hash_cache": "test",
            "os2sync.xfer_cpr": True,
            "os2sync.autowash": False,
            "os2sync.ca_verify_os2sync": True,
            "os2sync.ca_verify_os2mo": True,
            "os2sync.phone_scope_classes": [uuid4(), uuid4()],
            "os2sync.email_scope_classes": [uuid4(), uuid4()],
            "os2sync.ignored.unit_levels": [uuid4(), uuid4()],
            "os2sync.ignored.unit_types": [uuid4(), uuid4()],
            "os2sync.templates": {"template": "test"},
            "os2sync.use_contact_for_tasks": False,
        }
        with patch("integrations.os2sync.config.load_settings", return_value=conf):
            get_os2sync_settings.cache_clear()
            assert get_os2sync_settings()

    # Test that wrong value types raises a validation error
    @parameterized.expand(
        [
            ({"os2sync.api_url": "Not a URL"},),
            ({"mora.base": "Not a URL"},),
            ({"os2sync.use_lc_db": "No"},),
        ]
    )
    @patch("integrations.os2sync.config.load_settings", return_value=dummy_config)
    def test_invalid_values(self, wrong_type_config, settings_mock):
        settings_mock.return_value.update(wrong_type_config)
        with pytest.raises(ValidationError):
            get_os2sync_settings.cache_clear()
            get_os2sync_settings()
