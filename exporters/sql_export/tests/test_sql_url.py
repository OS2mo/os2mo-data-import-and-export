from functools import partial

import pytest

from exporters.sql_export.sql_url import (
    DatabaseFunction,
    generate_connection_url,
    generate_db_type_and_name,
    generate_engine_settings,
)


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"dummy": 1}, Exception("Configuration error, missing db name or type")),
        (
            {"exporters.actual_state.type": "SQLite"},
            Exception("Configuration error, missing db name or type"),
        ),
        (
            {"exporters.actual_state.db_name": "ActualState.db"},
            Exception("Configuration error, missing db name or type"),
        ),
        (
            {
                "exporters.actual_state.type": "SQLite",
                "exporters.actual_state.db_name": "ActualState",
            },
            ("SQLite", "ActualState"),
        ),
        (
            {
                "exporters.actual_state.type": "Postgres",
                "exporters.actual_state.db_name": "db0",
            },
            ("Postgres", "db0"),
        ),
    ],
)
def test_generate_db_type_and_name(settings, expected):
    test_func = partial(
        generate_db_type_and_name, DatabaseFunction.ACTUAL_STATE, False, settings
    )

    if isinstance(expected, Exception):
        with pytest.raises(Exception):
            test_func()
    else:
        result = test_func()
        assert result == expected


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"dummy": 1}, Exception("Configuration error, missing db name or type")),
        (
            {"exporters.actual_state.type": "SQLite"},
            {"pool_pre_ping": True},
        ),
        (
            {"exporters.actual_state.db_name": "ActualState.db"},
            Exception("Configuration error, missing db name or type"),
        ),
        (
            {
                "exporters.actual_state.type": "SQLite",
                "exporters.actual_state.db_name": "ActualState",
            },
            {"pool_pre_ping": True},
        ),
        (
            {
                "exporters.actual_state.type": "Mysql",
                "exporters.actual_state.db_name": "db0",
            },
            {"pool_pre_ping": True, "pool_recycle": 3600},
        ),
    ],
)
def test_generate_engine_settings(settings, expected):
    test_func = partial(
        generate_engine_settings, DatabaseFunction.ACTUAL_STATE, False, settings
    )

    if isinstance(expected, Exception):
        with pytest.raises(Exception):
            test_func()
    else:
        result = test_func()
        assert result == expected


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"dummy": 1}, Exception("Configuration error, missing db name or type")),
        (
            {"exporters.actual_state.type": "SQLite"},
            Exception("Configuration error, missing db name or type"),
        ),
        (
            {"exporters.actual_state.db_name": "ActualState.db"},
            Exception("Configuration error, missing db name or type"),
        ),
        (
            {
                "exporters.actual_state.type": "Memory",
                "exporters.actual_state.db_name": "Whatever",
            },
            "sqlite://",
        ),
        (
            {
                "exporters.actual_state.type": "SQLite",
                "exporters.actual_state.db_name": "ActualState",
            },
            "sqlite:///ActualState.db",
        ),
        (
            {
                "exporters.actual_state.type": "Postgres",
                "exporters.actual_state.db_name": "test0",
                "exporters.actual_state.user": "username",
                "exporters.actual_state.password": "password",
                "exporters.actual_state.host": "db",
            },
            "postgresql://username:password@db/test0",
        ),
    ],
)
def test_generate_connection_url(settings, expected):
    test_func = partial(
        generate_connection_url, DatabaseFunction.ACTUAL_STATE, False, settings
    )

    if isinstance(expected, Exception):
        with pytest.raises(Exception):
            test_func()
    else:
        result = test_func()
        assert result == expected
