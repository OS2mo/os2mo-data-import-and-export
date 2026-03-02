# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sql_export.sql_table_defs import ItForbindelse
from sql_export.sql_table_defs import ItSystem
from sqlalchemy.orm import Session

from .conftest import VALIDITY


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

    it_connection_uuid = await create_it_connection(
        {
            "user_key": "it_username",
            "person": person_uuid,
            "itsystem": it_system_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    it_system = one(actual_state_db_session.query(ItSystem).filter_by(uuid=it_system_uuid).all())
    assert it_system.navn == "My System"

    conn = one(actual_state_db_session.query(ItForbindelse).filter_by(uuid=it_connection_uuid).all())
    assert conn.it_system_uuid == it_system_uuid
    assert conn.bruger_uuid == person_uuid
    assert conn.brugernavn == "it_username"
