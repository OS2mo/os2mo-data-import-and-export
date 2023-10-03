# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0
import pytest
from payload_db import engine
from pytest import MonkeyPatch
from sqlalchemy.engine import Engine


def test_get_db_url_success(monkeypatch: MonkeyPatch) -> None:
    # Arrange
    for env_var in ("APP_DATABASE", "APP_DBUSER", "APP_DBPASSWORD", "PGHOST"):
        monkeypatch.setenv(env_var, env_var.lower())
    # Act
    db_url: str = engine.get_db_url()
    # Assert
    assert (
        db_url == "postgresql+psycopg2://app_dbuser:app_dbpassword@pghost/app_database"
    )


def test_get_db_url_failure(monkeypatch: MonkeyPatch) -> None:
    # Arrange
    for env_var in ("APP_DATABASE", "APP_DBUSER", "APP_DBPASSWORD", "PGHOST"):
        monkeypatch.delenv(env_var, raising=False)
    # Assert
    with pytest.raises(KeyError):
        # Act
        engine.get_db_url()


def test_get_engine(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(engine, "get_db_url", lambda: "sqlite://")
    result: Engine = engine.get_engine()
    assert isinstance(result, Engine)
