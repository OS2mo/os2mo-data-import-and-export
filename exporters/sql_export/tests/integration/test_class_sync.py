# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Klasse
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
@pytest.mark.clean_db
async def test_class_sync(
    trigger: Callable[[], Awaitable[None]],
    create_org: ...,
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    await create_org({"municipality_code": None})

    facet_uuid = await create_facet(
        {"user_key": "my_facet", "published": "Publiceret", "validity": VALIDITY}
    )

    class_uuid = await create_class(
        {
            "user_key": "my_class",
            "name": "My Class",
            "facet_uuid": facet_uuid,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )

    await trigger()

    classes = actual_state_db_session.query(Klasse).all()
    klasse = one(classes)
    assert klasse.bvn == "my_class"
    assert klasse.titel == "My Class"
    assert klasse.facet_uuid == facet_uuid
    assert klasse.facet_bvn == "my_facet"
