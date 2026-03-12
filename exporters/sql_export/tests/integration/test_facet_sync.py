# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Facet

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_facet_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    facet_user_key = "my_facet"
    facet_uuid = await create_facet(
        {"user_key": facet_user_key, "published": "Publiceret", "validity": VALIDITY}
    )

    await trigger()

    facet = one(actual_state_db_session.query(Facet).all())
    assert sql_to_dict(facet) == {
        "uuid": facet_uuid,
        "bvn": facet_user_key,
    }
