# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import os
from collections.abc import AsyncIterator

import pytest
from pytest import Item


def pytest_collection_modifyitems(items: list[Item]) -> None:
    """Fake `autouse` fixtures for tests marked with integration_test."""

    for item in items:
        if item.get_closest_marker("integration_test"):
            # MUST prepend to replicate auto-use fixtures coming first
            item.fixturenames[:0] = [  # type: ignore[attr-defined]
                # Default environmental variables for integration tests
                "integration_test_environment_variables",
                # Ensure Export DB is cleaned between integration tests
                "purge_export_db",
            ]


@pytest.fixture
def integration_test_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default environment for integration tests.

    Automatically used by tests marked 'integration_test' (see pytest_collection_modifyitems).

    GqlLoraCacheSettings (used by the app during startup) reads auth credentials
    from unprefixed env vars (CLIENT_SECRET, etc.), but docker-compose only sets
    FASTRAMQPI__-prefixed versions. Mirror the FASTRAMQPI__ auth settings so both
    the app and test fixtures can find them.
    """
    # (source FASTRAMQPI__ env var, default if source is unset)
    env_mappings = {
        "CLIENT_ID": ("FASTRAMQPI__CLIENT_ID", "dipex"),
        "CLIENT_SECRET": ("FASTRAMQPI__CLIENT_SECRET", None),
        "AUTH_SERVER": ("FASTRAMQPI__AUTH_SERVER", "http://keycloak:8080/auth"),
        "AUTH_REALM": ("FASTRAMQPI__AUTH_REALM", "mo"),
    }
    for target, (source, default) in env_mappings.items():
        value = os.environ.get(source, default)
        if value is not None:
            monkeypatch.setenv(target, value)


@pytest.fixture
async def purge_export_db() -> AsyncIterator[None]:
    yield
