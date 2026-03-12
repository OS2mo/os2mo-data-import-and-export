# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Leder
from sql_export.sql_table_defs import LederAnsvar

from .conftest import VALIDITY
from .conftest import sql_to_dict


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

    manager = one(actual_state_db_session.query(Leder).all())
    assert sql_to_dict(manager) == {
        "uuid": manager_uuid,
        "bruger_uuid": person_uuid,
        "enhed_uuid": unit_uuid,
        "ledertype_uuid": manager_type_uuid,
        "ledertype_titel": "Leader",
        "niveautype_uuid": manager_level_uuid,
        "niveautype_titel": "Level 1",
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }

    responsibility = one(actual_state_db_session.query(LederAnsvar).all())
    assert sql_to_dict(responsibility) == {
        "leder_uuid": manager_uuid,
        "lederansvar_uuid": responsibility_uuid,
        "lederansvar_titel": "Responsibility 1",
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
