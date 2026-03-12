# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import os
from collections.abc import AsyncIterator
from collections.abc import Iterator

import pytest
from httpx import AsyncClient
from pytest import Item
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Base


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[Item]) -> None:
    """Fake `autouse` fixtures for tests marked with integration_test.

    Uses trylast=True so it runs after the fastramqpi plugin's hook, ensuring
    prepended fixtures (like empty_db) end up before the plugin's fixtures.
    """

    for item in items:
        if item.get_closest_marker("integration_test"):
            # MUST prepend to replicate auto-use fixtures coming first
            item.fixturenames[:0] = [  # type: ignore[attr-defined]
                # Ensure MO database is clean before snapshot/background tasks
                "empty_db",
                # Default environmental variables for integration tests
                "integration_test_environment_variables",
                # Ensure Export DB is cleaned between integration tests
                "purge_export_db",
            ]


@pytest.fixture
async def empty_db(
    unauthenticated_mo_client: AsyncClient,
) -> AsyncIterator[None]:
    """Ensure tests are running on an empty database."""
    r = await unauthenticated_mo_client.post("/testing/database/purge")
    r.raise_for_status()
    yield


@pytest.fixture
def integration_test_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default environment for integration tests.

    Automatically used by tests marked 'integration_test' (see pytest_collection_modifyitems).
    """
    pass


@pytest.fixture
def purge_export_db() -> Iterator[None]:
    """Truncate all tables in the export DB before each integration test."""
    db_user = os.environ["ACTUAL_STATE__USER"]
    db_pass = os.environ["ACTUAL_STATE__PASSWORD"]
    db_host = os.environ["ACTUAL_STATE__HOST"]
    db_port = os.environ.get("ACTUAL_STATE__PORT", "5432")
    db_name = os.environ["ACTUAL_STATE__DB_NAME"]

    url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(text(f"TRUNCATE TABLE {table.name} CASCADE"))
        session.commit()
    yield
