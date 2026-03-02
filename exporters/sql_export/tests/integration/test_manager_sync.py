# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Leder
from sql_export.sql_table_defs import LederAnsvar
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_manager_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    manager_type_facet: UUID,
    manager_level_facet: UUID,
    responsibility_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_manager: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    manager_type_uuid = await create_class(
        {
            "user_key": "leader",
            "name": "Leader",
            "facet_uuid": str(manager_type_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    manager_level_uuid = await create_class(
        {
            "user_key": "level1",
            "name": "Level 1",
            "facet_uuid": str(manager_level_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    responsibility_uuid = await create_class(
        {
            "user_key": "resp1",
            "name": "Responsibility 1",
            "facet_uuid": str(responsibility_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
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

    person_uuid = await create_person(
        {
            "cpr_number": "0505700000",
            "given_name": "Manager",
            "surname": "User",
            "user_key": "manager_user",
        }
    )
    unit_uuid = await create_org_unit(
        {
            "user_key": "manager_unit",
            "name": "Manager Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    manager_uuid = await create_manager(
        {
            "person": person_uuid,
            "org_unit": unit_uuid,
            "manager_type": manager_type_uuid,
            "manager_level": manager_level_uuid,
            "responsibility": [responsibility_uuid],
            "validity": VALIDITY,
        }
    )

    await trigger()

    mgr = one(actual_state_db_session.query(Leder).filter_by(uuid=manager_uuid).all())
    assert mgr.bruger_uuid == person_uuid
    assert mgr.enhed_uuid == unit_uuid
    assert mgr.ledertype_uuid == manager_type_uuid
    assert mgr.niveautype_uuid == manager_level_uuid

    responsibility = one(actual_state_db_session.query(LederAnsvar).filter_by(leder_uuid=manager_uuid).all())
    assert responsibility.lederansvar_uuid == responsibility_uuid
