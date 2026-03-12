# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import ItSystem

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_it_system_sync(
    trigger: Callable[[], Awaitable[None]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    input_data = {
        "name": "My System",
        "user_key": "my_system",
        "validity": VALIDITY,
    }
    it_system_uuid = await create_it_system(input_data)

    await trigger()

    it_system = one(actual_state_db_session.query(ItSystem).all())
    assert sql_to_dict(it_system) == {
        "uuid": it_system_uuid,
        "navn": input_data["name"],
    }
