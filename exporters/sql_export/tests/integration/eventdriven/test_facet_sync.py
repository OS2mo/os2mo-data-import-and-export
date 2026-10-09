# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from fastapi import FastAPI
from fastramqpi.pytest_plugin import run_server
from fastramqpi.pytest_plugin import run_test_client
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Facet

from ..conftest import VALIDITY
from ..conftest import sql_to_dict
from .conftest import trigger_actualstate_sync


@pytest.mark.integration_test
async def test_facet_actualstate_event(
    app: FastAPI,
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Call the /events/actualstate/facet route directly, bypassing the
    GraphQL event listener, and assert the export DB is updated accordingly.
    """
    # Arrange
    facet_user_key = "my_facet"
    facet_uuid = await create_facet({"user_key": facet_user_key, "validity": VALIDITY})

    # Act
    async with run_server(app), run_test_client() as test_client:
        await trigger_actualstate_sync(test_client, "facet", facet_uuid)

    # Assert
    facet = one(actual_state_db_session.query(Facet).all())
    assert sql_to_dict(facet) == {
        "uuid": facet_uuid,
        "bvn": facet_user_key,
    }
