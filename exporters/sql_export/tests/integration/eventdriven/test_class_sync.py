# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from httpx import AsyncClient
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Klasse

from ..conftest import VALIDITY
from ..conftest import sql_to_dict


@pytest.mark.integration_test
async def test_class_actualstate_event(
    test_client: AsyncClient,
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Create a class and call the /events/actualstate/class and check that the class is exported correctly."""
    facet_user_key = "my_facet"
    class_user_key = "my_class"
    class_name = "klasse"
    facet_uuid = await create_facet({"user_key": facet_user_key, "validity": VALIDITY})

    class_uuid = await create_class(
        {
            "user_key": class_user_key,
            "name": class_name,
            "validity": VALIDITY,
            "facet_uuid": facet_uuid,
        }
    )
    response = await test_client.post(
        "/events/actualstate/class",
        json={"subject": class_uuid, "priority": 0},
    )
    assert response.status_code == 200

    klasse = one(actual_state_db_session.query(Klasse).all())
    assert sql_to_dict(klasse) == {
        "uuid": class_uuid,
        "bvn": class_user_key,
        "titel": class_name,
        "facet_uuid": facet_uuid,
        "facet_bvn": facet_user_key,
    }
