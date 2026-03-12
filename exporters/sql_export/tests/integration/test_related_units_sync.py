# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from unittest.mock import ANY
from uuid import UUID

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Enhedssammenkobling

from .conftest import VALIDITY
from .conftest import sql_to_dict


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

    await create_related(
        {
            "origin": unit1_uuid,
            "destination": [unit2_uuid],
            "validity": VALIDITY,
        }
    )

    await trigger()

    related = one(actual_state_db_session.query(Enhedssammenkobling).all())
    # org_unit_uuids from MO GraphQL is sorted, so enhed1 < enhed2
    enhed1_uuid, enhed2_uuid = sorted([unit1_uuid, unit2_uuid])
    assert sql_to_dict(related) == {
        # create_related returns the origin org unit UUID (enhed1_uuid),
        # not the relation UUID itself, so we cannot assert the exact value here.
        "uuid": ANY,
        "enhed1_uuid": enhed1_uuid,
        "enhed2_uuid": enhed2_uuid,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
