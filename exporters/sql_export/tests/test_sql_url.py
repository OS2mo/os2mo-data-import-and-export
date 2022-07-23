from functools import partial

import pytest

from ..config import ConfigurationError
from ..config import Settings
from ..sql_url import DatabaseFunction
from ..sql_url import generate_connection_url
from ..sql_url import generate_engine_settings


@pytest.mark.parametrize(
    "settings_overrides,expected_type,expected_historic_type",
    [
        ({}, "Postgres", "Postgres"),
        ({"sql_export_type": "Alfa"}, "Alfa", "Alfa"),
        ({"sql_export_historic_type": "Beta"}, "Postgres", "Beta"),
        (
            {"sql_export_type": "Alfa", "sql_export_historic_type": "Beta"},
            "Alfa",
            "Beta",
        ),
    ],
)
def test_settings(
    settings_overrides: dict[str, str], expected_type: str, expected_historic_type: str
) -> None:
    settings = Settings(**settings_overrides)
    assert settings.sql_export_type == expected_type
    assert settings.sql_export_historic_type == expected_historic_type


@pytest.mark.parametrize(
    "settings_overrides,expected",
    [
        ({}, ("Postgres", "actualstate")),
        (
            {"sql_export_type": ""},
            ConfigurationError("Missing value in db_type"),
        ),
        (
            {"sql_export_db_name": ""},
            ConfigurationError("Missing value in db_name"),
        ),
        (
            {
                "sql_export_type": "SQLite",
                "sql_export_db_name": "ActualState",
            },
            ("SQLite", "ActualState"),
        ),
        (
            {
                "sql_export_type": "Postgres",
                "sql_export_db_name": "db0",
            },
            ("Postgres", "db0"),
        ),
    ],
)
def test_generate_db_type_and_name(settings_overrides, expected):
    settings = Settings(**settings_overrides)
    try:
        db_type = settings.get_db_type(DatabaseFunction.ACTUAL_STATE, False)
        db_name = settings.get_db_name(DatabaseFunction.ACTUAL_STATE)
        assert (db_type, db_name) == expected
    except AssertionError as exp:
        raise exp
    except Exception as exp:
        assert type(exp) == type(expected)
        assert str(exp) == str(expected)


@pytest.mark.parametrize(
    "settings_overrides,expected",
    [
        ({}, {"pool_pre_ping": True}),
        (
            {"sql_export_type": ""},
            ConfigurationError("Missing value in db_type"),
        ),
        (
            {"sql_export_db_name": ""},
            {"pool_pre_ping": True},
        ),
        (
            {"sql_export_type": "SQLite"},
            {"pool_pre_ping": True},
        ),
        (
            {"sql_export_db_name": "ActualState.db"},
            {"pool_pre_ping": True},
        ),
        (
            {
                "sql_export_type": "SQLite",
                "sql_export_db_name": "ActualState.db",
            },
            {"pool_pre_ping": True},
        ),
        (
            {
                "sql_export_type": "Mysql",
                "sql_export_db_name": "db0",
            },
            {"pool_pre_ping": True, "pool_recycle": 3600},
        ),
    ],
)
def test_generate_engine_settings(settings_overrides, expected):
    settings = Settings(**settings_overrides)
    test_func = partial(
        generate_engine_settings, DatabaseFunction.ACTUAL_STATE, False, settings
    )

    try:
        result = test_func()
        assert result == expected
    except AssertionError as exp:
        raise exp
    except Exception as exp:
        assert type(exp) == type(expected)
        assert str(exp) == str(expected)


@pytest.mark.parametrize(
    "settings_overrides,expected",
    [
        ({}, "postgresql://postgres:@localhost/actualstate"),
        (
            {"sql_export_type": ""},
            ConfigurationError("Missing value in db_type"),
        ),
        (
            {"sql_export_db_name": ""},
            ConfigurationError("Missing value in db_name"),
        ),
        (
            {
                "sql_export_type": "Memory",
                "sql_export_db_name": "Whatever",
            },
            "sqlite://",
        ),
        (
            {
                "sql_export_type": "SQLite",
                "sql_export_db_name": "ActualState",
            },
            "sqlite:///ActualState.db",
        ),
        (
            {
                "sql_export_type": "Postgres",
                "sql_export_db_name": "test0",
                "sql_export_user": "username",
                "sql_export_password": "password",
                "sql_export_host": "db",
            },
            "postgresql://username:password@db/test0",
        ),
    ],
)
def test_generate_connection_url(settings_overrides, expected):
    settings = Settings(**settings_overrides)
    test_func = partial(
        generate_connection_url, DatabaseFunction.ACTUAL_STATE, False, settings
    )

    try:
        result = test_func()
        assert result == expected
    except AssertionError as exp:
        raise exp
    except Exception as exp:
        assert type(exp) == type(expected)
        assert str(exp) == str(expected)
