# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Facet
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_facet_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    facet_uuid = await create_facet(
        {"user_key": "my_facet", "published": "Publiceret", "validity": VALIDITY}
    )

    await trigger()

    facet = one(actual_state_db_session.query(Facet).filter_by(uuid=facet_uuid).all())
    assert facet.bvn == "my_facet"
