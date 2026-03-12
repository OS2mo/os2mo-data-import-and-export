# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Bruger

from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_employee_sync(
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

    user = one(actual_state_db_session.query(Bruger).all())
    assert sql_to_dict(user) == {
        "uuid": person_uuid,
        "bvn": input_data["user_key"],
        "fornavn": input_data["given_name"],
        "efternavn": input_data["surname"],
        "kaldenavn_fornavn": "",
        "kaldenavn_efternavn": "",
        "cpr": input_data["cpr_number"],
        "startdato": "1970-01-01",
        "slutdato": "9999-12-31",
    }
