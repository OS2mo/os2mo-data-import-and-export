# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Fixtures shared by the event-driven integration tests."""

import os

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from sql_export.main import create_app


@pytest.fixture
def app(load_marked_envvars: None) -> FastAPI:
    # Ensure EVENTDRIVEN is True. This ensures the /events/{...} routes are registered
    os.environ["EVENTDRIVEN"] = "true"
    return create_app()


async def trigger_sync(
    test_client: AsyncClient, state: str, mo_type: str, uuid: str
) -> None:
    response = await test_client.post(
        f"/events/{state}/{mo_type}",
        json={"subject": uuid, "priority": 0},
    )
    assert response.status_code == 200


async def trigger_actualstate_sync(
    test_client: AsyncClient, mo_type: str, uuid: str
) -> None:
    await trigger_sync(test_client, "actualstate", mo_type, uuid)


async def trigger_historic_sync(
    test_client: AsyncClient, mo_type: str, uuid: str
) -> None:
    await trigger_sync(test_client, "historic", mo_type, uuid)
