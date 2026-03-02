# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Tilknytning
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_association_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_association: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    association_type_facet = await create_facet(
        {"user_key": "association_type", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "published": "Publiceret", "validity": VALIDITY}
    )

    association_type_uuid = await create_class(
        {
            "user_key": "assoc_type",
            "name": "Assoc Type",
            "facet_uuid": association_type_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    unit_type_uuid = await create_class(
        {
            "user_key": "unit_type",
            "name": "Unit Type",
            "facet_uuid": org_unit_type_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    level_uuid = await create_class(
        {
            "user_key": "level",
            "name": "Level",
            "facet_uuid": org_unit_level_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )

    person_uuid = await create_person(
        {
            "cpr_number": "0606700000",
            "given_name": "Assoc",
            "surname": "User",
            "user_key": "assoc_user",
        }
    )
    unit_uuid = await create_org_unit(
        {
            "user_key": "assoc_unit",
            "name": "Assoc Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    association_uuid = await create_association(
        {
            "person": person_uuid,
            "org_unit": unit_uuid,
            "association_type": association_type_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    association = one(actual_state_db_session.query(Tilknytning).all())
    assert association.uuid == association_uuid
    assert association.bruger_uuid == person_uuid
    assert association.enhed_uuid == unit_uuid
    assert association.tilknytningstype_uuid == association_type_uuid
