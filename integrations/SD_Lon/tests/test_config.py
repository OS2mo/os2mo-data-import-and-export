import json
from copy import deepcopy
from unittest.mock import patch

import pytest
from parameterized import parameterized
from pydantic import BaseSettings
from pydantic import ValidationError

from integrations.SD_Lon.config import get_settings
from integrations.SD_Lon.config import json_file_settings
from integrations.SD_Lon.config import Settings

DEFAULT_MOCK_SETTINGS = {
    "integrations.SD_Lon.employment_field": "extension_1",
    "integrations.SD_Lon.global_from_date": "2022-01-09",
    "integrations.SD_Lon.import.run_db": "run_db.sqlite",
    "integrations.SD_Lon.institution_identifier": "XYZ",
    "integrations.SD_Lon.job_function": "EmploymentName",
    "integrations.SD_Lon.monthly_hourly_divide": 50000,
    "integrations.SD_Lon.sd_user": "user",
    "integrations.SD_Lon.sd_password": "password",
    "municipality.code": "740",
    "municipality.cvr": 29189641,
    "municipality.name": "Kolding Kommune",
}

DEFAULT_EXPECTED_SETTINGS = {
    "sd_employment_field": "extension_1",
    "sd_global_from_date": "2022-01-09",
    "sd_import_run_db": "run_db.sqlite",
    "sd_institution_identifier": "XYZ",
    "sd_job_function": "EmploymentName",
    "sd_monthly_hourly_divide": 50000,
    "sd_user": "user",
    "sd_password": "password",
    "municipality_code": "740",
    "municipality_cvr": 29189641,
    "municipality_name": "Kolding Kommune",
    "mora_base": "http://mo-service:5000",
    "mox_base": "http://mox-service:8080",
    "sd_import_too_deep": [],
    "sd_importer_create_associations": True,
    "sd_importer_create_email_addresses": True,
    "sd_importer_employment_date_as_engagement_start_date": False,
    "sd_skip_employment_types": [],
    "sd_terminate_engagement_with_to_only": True,
    "sd_use_ad_integration": False,
}


@pytest.fixture
def mock_env_and_json(monkeypatch):
    monkeypatch.setenv("SD_USER", "env_user")
    monkeypatch.setattr(
        "integrations.SD_Lon.config.load_settings", lambda: DEFAULT_MOCK_SETTINGS
    )


@patch("integrations.SD_Lon.config.load_settings")
def test_json_file_settings(mock_load_settings):
    # Arrange
    mock_load_settings.return_value = DEFAULT_MOCK_SETTINGS

    # Act
    settings = json_file_settings(BaseSettings())

    # Assert
    assert settings == {
        "sd_employment_field": "extension_1",
        "sd_global_from_date": "2022-01-09",
        "sd_import_run_db": "run_db.sqlite",
        "sd_institution_identifier": "XYZ",
        "sd_job_function": "EmploymentName",
        "sd_monthly_hourly_divide": 50000,
        "sd_user": "user",
        "sd_password": "password",
        "municipality_code": "740",
        "municipality_cvr": 29189641,
        "municipality_name": "Kolding Kommune",
    }


@patch("integrations.SD_Lon.config.load_settings")
def test_empty_dict_on_file_not_found_error(mock_load_settings):
    # Arrange
    mock_load_settings.side_effect = FileNotFoundError()

    # Act
    json_settings = json_file_settings(BaseSettings())

    # Assert
    assert json_settings == dict()


@patch("integrations.SD_Lon.config.load_settings")
def test_extra_settings_ignored_and_defaults_set(mock_load_settings):
    # Arrange
    mock_settings = deepcopy(DEFAULT_MOCK_SETTINGS)
    mock_settings.update({"unknown": "setting"})
    mock_load_settings.return_value = mock_settings

    # Act
    get_settings.cache_clear()
    settings = get_settings()

    # Assert
    assert json.loads(settings.json()) == DEFAULT_EXPECTED_SETTINGS


def test_env_settings_takes_precedence(mock_env_and_json):
    # Act
    get_settings.cache_clear()
    settings = get_settings()

    # Assert
    assert settings.sd_user == "env_user"


@patch("integrations.SD_Lon.config.load_settings")
def test_override_default(mock_load_settings):
    # Arrange
    mock_settings = deepcopy(DEFAULT_MOCK_SETTINGS)
    mock_settings.update({"integrations.SD_Lon.sd_importer.create_associations": False})
    mock_load_settings.return_value = mock_settings

    # Act
    get_settings.cache_clear()
    settings = get_settings()

    # Assert
    assert not settings.sd_importer_create_associations


@parameterized.expand(
    [
        ("mora_base", "Not a URL"),
        ("mox_base", "Not a URL"),
        ("municipality_cvr", -1),
        ("sd_global_from_date", "Invalid string"),
        ("sd_monthly_hourly_divide", -1),
    ]
)
def test_special_values(key, value):
    # Arrange
    mock_settings = deepcopy(DEFAULT_MOCK_SETTINGS)
    mock_settings[key] = value

    # Act and assert
    with pytest.raises(ValidationError):
        Settings.parse_obj(mock_settings)
