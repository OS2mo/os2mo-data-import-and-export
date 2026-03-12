# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Klasse

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_class_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    facet_input = {
        "user_key": "my_facet",
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    facet_uuid = await create_facet(facet_input)

    class_input = {
        "user_key": "my_class",
        "name": "My Class",
        "facet_uuid": facet_uuid,
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    class_uuid = await create_class(class_input)

    await trigger()

    klasse = one(actual_state_db_session.query(Klasse).all())
    assert sql_to_dict(klasse) == {
        "uuid": class_uuid,
        "bvn": class_input["user_key"],
        "titel": class_input["name"],
        "facet_uuid": facet_uuid,
        "facet_bvn": facet_input["user_key"],
    }
