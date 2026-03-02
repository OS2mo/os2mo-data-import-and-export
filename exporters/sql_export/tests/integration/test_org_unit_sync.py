# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Enhed
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_org_unit_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    unit_type_uuid = await create_class(
        {
            "user_key": "unit_type",
            "name": "Unit Type",
            "facet_uuid": str(org_unit_type_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    level_uuid = await create_class(
        {
            "user_key": "level",
            "name": "Level",
            "facet_uuid": str(org_unit_level_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )

    input_data = {
        "user_key": "my_unit",
        "name": "My Unit",
        "org_unit_type": unit_type_uuid,
        "org_unit_level": level_uuid,
        "validity": VALIDITY,
    }
    unit_uuid = await create_org_unit(input_data)

    await trigger()

    unit = one(actual_state_db_session.query(Enhed).all())
    assert unit.uuid == unit_uuid
    assert unit.bvn == input_data["user_key"]
    assert unit.navn == input_data["name"]
    assert unit.enhedstype_uuid == unit_type_uuid
    assert unit.enhedsniveau_uuid == level_uuid
