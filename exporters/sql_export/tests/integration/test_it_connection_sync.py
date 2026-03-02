# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
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

    it_systems = actual_state_db_session.query(ItSystem).all()
    found_sys = next((s for s in it_systems if s.uuid == it_system_uuid), None)
    assert found_sys is not None
    assert found_sys.navn == "My System"

    it_connections = actual_state_db_session.query(ItForbindelse).all()
    found_conn = next((c for c in it_connections if c.uuid == it_connection_uuid), None)

    assert found_conn is not None
    assert found_conn.it_system_uuid == it_system_uuid
    assert found_conn.bruger_uuid == person_uuid
    assert found_conn.brugernavn == "it_username"
