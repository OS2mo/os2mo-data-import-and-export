import json
from copy import deepcopy
from typing import Any
from typing import Dict
from unittest.mock import patch

import pytest
from parameterized import parameterized
from pydantic import BaseSettings
from pydantic import ValidationError

from integrations.SD_Lon.config import gen_json_file_settings_func
from integrations.SD_Lon.config import get_importer_settings
from integrations.SD_Lon.config import ImporterSettings


importer_json_file_settings = gen_json_file_settings_func(ImporterSettings)

DEFAULT_MOCK_SETTINGS = {
    "integrations.SD_Lon.employment_field": "extension_1",
    "integrations.SD_Lon.global_from_date": "2022-01-09",
    "integrations.SD_Lon.import.run_db": "run_db.sqlite",
    "integrations.SD_Lon.institution_identifier": "XYZ",
    "integrations.SD_Lon.job_function": "EmploymentName",
    "integrations.SD_Lon.monthly_hourly_divide": 50000,
    "integrations.SD_Lon.sd_user": "user",
    "integrations.SD_Lon.sd_password": "password",
    "municipality.code": 740,
    "municipality.name": "Kolding Kommune",
}

DEFAULT_EXPECTED_SETTINGS: Dict[str, Any] = {
    "sd_employment_field": "extension_1",
    "sd_global_from_date": "2022-01-09",
    "sd_import_run_db": "run_db.sqlite",
    "sd_institution_identifier": "XYZ",
    "sd_job_function": "EmploymentName",
    "sd_no_salary_minimum_id": None,
    "sd_monthly_hourly_divide": 50000,
    "sd_user": "user",
    "sd_password": "**********",
    "municipality_code": 740,
    "municipality_name": "Kolding Kommune",
    "mora_base": "http://mo-service:5000",
    "mox_base": "http://mox-service:8080",
    "sd_import_too_deep": [],
    "sd_importer_create_associations": True,
    "sd_importer_create_email_addresses": True,
    "sd_importer_employment_date_as_engagement_start_date": False,
    "sd_skip_job_functions": [],
    "sd_skip_employment_types": [],
    "sd_terminate_engagement_with_to_only": True,
    "sd_use_ad_integration": True,
}

DEFAULT_FILTERED_JSON_SETTINGS = {
    "sd_employment_field": "extension_1",
    "sd_global_from_date": "2022-01-09",
    "sd_import_run_db": "run_db.sqlite",
    "sd_institution_identifier": "XYZ",
    "sd_job_function": "EmploymentName",
    "sd_monthly_hourly_divide": 50000,
    "sd_user": "user",
    "sd_password": "password",
    "municipality_code": 740,
    "municipality_name": "Kolding Kommune",
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
    settings = importer_json_file_settings(BaseSettings())

    # Assert
    assert settings == DEFAULT_FILTERED_JSON_SETTINGS


@patch("integrations.SD_Lon.config.load_settings")
def test_json_file_settings_remove_unknown_settings(mock_load_settings):
    # Arrange
    mock_settings = deepcopy(DEFAULT_MOCK_SETTINGS)
    mock_settings.update({"unknown": "property"})
    mock_load_settings.return_value = mock_settings

    # Act
    settings = importer_json_file_settings(BaseSettings())

    # Assert
    assert settings == DEFAULT_FILTERED_JSON_SETTINGS


@patch("integrations.SD_Lon.config.load_settings")
def test_empty_dict_on_file_not_found_error(mock_load_settings):
    # Arrange
    mock_load_settings.side_effect = FileNotFoundError()

    # Act
    json_settings = importer_json_file_settings(BaseSettings())

    # Assert
    assert json_settings == dict()


@patch("integrations.SD_Lon.config.load_settings")
def test_set_defaults(mock_load_settings):
    # Arrange
    mock_load_settings.return_value = DEFAULT_MOCK_SETTINGS

    # Act
    get_importer_settings.cache_clear()
    settings_input = get_importer_settings()

    # Assert
    assert json.loads(settings_input.json()) == DEFAULT_EXPECTED_SETTINGS


def test_forbid_extra_settings():
    with pytest.raises(ValidationError):
        ImporterSettings(
            municipality_name="name",
            municipality_code=100,
            sd_global_from_date="1970-01-01",
            sd_employment_field="extension_1",
            sd_import_run_db="run_db.sqlite",
            sd_institution_identifier="XY",
            sd_job_function="EmploymentName",
            sd_monthly_hourly_divide=9000,
            sd_password="secret",
            sd_user="user",
            forbidden="property",
        )


def test_env_settings_takes_precedence(mock_env_and_json):
    # Act
    get_importer_settings.cache_clear()
    settings = get_importer_settings()

    # Assert
    assert settings.sd_user == "env_user"


@patch("integrations.SD_Lon.config.load_settings")
def test_override_default(mock_load_settings):
    # Arrange
    mock_settings = deepcopy(DEFAULT_MOCK_SETTINGS)
    mock_settings.update({"integrations.SD_Lon.sd_importer.create_associations": False})
    mock_load_settings.return_value = mock_settings

    # Act
    get_importer_settings.cache_clear()
    settings = get_importer_settings()

    # Assert
    assert not settings.sd_importer_create_associations


@parameterized.expand(
    [
        ("mora_base", "Not a URL"),
        ("mox_base", "Not a URL"),
        ("municipality_code", 98),
        ("municipality_code", 1000),
        ("sd_employment_field", "extension_"),
        ("sd_employment_field", "Invalid string"),
        ("sd_global_from_date", "Invalid string"),
        ("sd_job_function", "not allowed"),
        ("sd_monthly_hourly_divide", -1),
    ]
)
def test_special_values(key, value):
    # Arrange
    mock_settings = deepcopy(DEFAULT_MOCK_SETTINGS)
    mock_settings[key] = value

    # Act and assert
    with pytest.raises(ValidationError):
        ImporterSettings.parse_obj(mock_settings)


@parameterized.expand(["JobPositionIdentifier", "EmploymentName"])
def test_job_function_enums_allowed(job_function):
    assert ImporterSettings(
        municipality_name="name",
        municipality_code=100,
        sd_global_from_date="1970-01-01",
        sd_employment_field="extension_1",
        sd_import_run_db="run_db.sqlite",
        sd_institution_identifier="XY",
        sd_job_function=job_function,
        sd_monthly_hourly_divide=9000,
        sd_password="secret",
        sd_user="user",
    )
