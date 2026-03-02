# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Enhedssammenkobling
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_related_units_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_related: Callable[[dict[str, Any]], Awaitable[str]],
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

    unit1_uuid = await create_org_unit(
        {
            "user_key": "unit_1",
            "name": "Unit 1",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )
    unit2_uuid = await create_org_unit(
        {
            "user_key": "unit_2",
            "name": "Unit 2",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    related_uuid = await create_related(
        {
            "origin": unit1_uuid,
            "destination": [unit2_uuid],
            "validity": VALIDITY,
        }
    )

    await trigger()

    related = one(actual_state_db_session.query(Enhedssammenkobling).all())
    assert {related.enhed1_uuid, related.enhed2_uuid} == {unit1_uuid, unit2_uuid}
