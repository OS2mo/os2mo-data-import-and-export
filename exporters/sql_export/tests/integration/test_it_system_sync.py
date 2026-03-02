# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sql_export.sql_table_defs import ItSystem
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_it_system_sync(
    trigger: Callable[[], Awaitable[None]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    it_system_uuid = await create_it_system(
        {
            "name": "My System",
            "user_key": "my_system",
            "validity": VALIDITY,
        }
    )

    await trigger()

    it_system = one(actual_state_db_session.query(ItSystem).filter_by(uuid=it_system_uuid).all())
    assert it_system.navn == "My System"
