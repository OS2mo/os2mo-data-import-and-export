# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from httpx import AsyncClient
from more_itertools import one
from sql_export.sql_table_defs import Bruger
from sqlalchemy.orm import Session


@pytest.mark.integration_test
def test_trigger1() -> None:
    pass


@pytest.fixture
def trigger(test_client: AsyncClient) -> Callable[[], Awaitable[None]]:
    async def inner() -> None:
        response = await test_client.post(
            "/trigger",
            params={
                "resolve_dar": False,
                "historic": False,
                "read_from_cache": False,
            },
        )
        assert response.status_code == 200
        assert response.json() == {"detail": "Triggered"}

        response = await test_client.post(
            "/wait_for_finish",
            params={"historic": False},
            timeout=60.0,
        )
        assert response.status_code == 200
        assert response.json() == {"detail": "Finished"}

    return inner


@pytest.mark.integration_test
async def test_trigger_full_export(
    trigger: Callable[[], Awaitable[None]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    input_data = {
        "cpr_number": "0101700000",
        "given_name": "given_name",
        "surname": "surname",
        "user_key": "user_key",
    }
    person_uuid = await create_person(input_data)

    await trigger()

    # Read all users and assert there is one and only one
    users = actual_state_db_session.query(Bruger).all()
    user = one(users)
    # Assert that the read user has the expected data
    assert user.uuid == person_uuid
    assert user.fornavn == input_data["given_name"]
    assert user.efternavn == input_data["surname"]
    assert user.cpr == input_data["cpr_number"]
    assert user.bvn == input_data["user_key"]
