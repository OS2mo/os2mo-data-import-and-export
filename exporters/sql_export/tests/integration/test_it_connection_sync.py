# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import ItForbindelse

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_it_connection_sync(
    trigger: Callable[[], Awaitable[None]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    person_uuid = await create_person(
        {
            "cpr_number": "0404700000",
            "given_name": "IT",
            "surname": "User",
            "user_key": "it_user",
        }
    )
    it_system_uuid = await create_it_system(
        {
            "name": "My System",
            "user_key": "my_system",
            "validity": VALIDITY,
        }
    )

    connection_input = {
        "user_key": "it_username",
        "person": person_uuid,
        "itsystem": it_system_uuid,
        "validity": VALIDITY,
    }
    it_connection_uuid = await create_it_connection(connection_input)

    await trigger()

    conn = one(actual_state_db_session.query(ItForbindelse).all())
    assert sql_to_dict(conn) == {
        "uuid": it_connection_uuid,
        "it_system_uuid": it_system_uuid,
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "brugernavn": connection_input["user_key"],
        "eksternt_id": None,
        "primær_boolean": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_it_connection_external_id_sync(
    trigger: Callable[[], Awaitable[None]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    person_uuid = await create_person(
        {
            "cpr_number": "0404700000",
            "given_name": "IT",
            "surname": "User",
            "user_key": "it_user",
        }
    )
    it_system_uuid = await create_it_system(
        {
            "name": "My System",
            "user_key": "my_system",
            "validity": VALIDITY,
        }
    )

    connection_input = {
        "user_key": "it_username",
        "person": person_uuid,
        "itsystem": it_system_uuid,
        "external_id": "ext-12345",
        "validity": VALIDITY,
    }
    it_connection_uuid = await create_it_connection(connection_input)

    await trigger()

    conn = one(actual_state_db_session.query(ItForbindelse).all())
    assert sql_to_dict(conn) == {
        "uuid": it_connection_uuid,
        "it_system_uuid": it_system_uuid,
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "brugernavn": connection_input["user_key"],
        "eksternt_id": connection_input["external_id"],
        "primær_boolean": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
