# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import os
from collections.abc import Iterator

import pytest
from pytest import Item
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Base


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[Item]) -> None:
    """Fake `autouse` fixtures for tests marked with integration_test.

    Uses trylast=True so it runs after the fastramqpi plugin's hook, ensuring
    our prepended fixtures end up before the plugin's fixtures.

    Cleaning the MO database is not listed here: the fastramqpi plugin prepends
    its own `os2mo_database_isolation` fixture for integration_test items.
    """

    for item in items:
        if item.get_closest_marker("integration_test"):
            # MUST prepend to replicate auto-use fixtures coming first
            item.fixturenames[:0] = [  # type: ignore[attr-defined]
                # Default environmental variables for integration tests
                "integration_test_environment_variables",
            ]


@pytest.fixture
def integration_test_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default environment for integration tests.

    Automatically used by tests marked 'integration_test' (see pytest_collection_modifyitems).
    """
    pass


def purge_export_db(prefix: str) -> Iterator[Session]:
    """Truncate all tables in the export DB before each integration test."""
    db_user = os.environ[f"{prefix}__USER"]
    db_pass = os.environ[f"{prefix}__PASSWORD"]
    db_host = os.environ[f"{prefix}__HOST"]
    db_port = os.environ.get(f"{prefix}__PORT", "5432")
    db_name = os.environ[f"{prefix}__DB_NAME"]
    url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(text(f"TRUNCATE TABLE {table.name} CASCADE"))
        session.commit()
        yield session


@pytest.fixture
def actual_state_db_session() -> Iterator[Session]:
    yield from purge_export_db("ACTUAL_STATE")


@pytest.fixture
def historic_state_db_session() -> Iterator[Session]:
    yield from purge_export_db("HISTORIC_STATE")
