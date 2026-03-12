# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Tilknytning

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_association_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    association_type_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_association: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    association_type_uuid = await create_class(
        {
            "user_key": "assoc_type",
            "name": "Assoc Type",
            "facet_uuid": str(association_type_facet),
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
            "user_key": "my_association",
            "person": person_uuid,
            "org_unit": unit_uuid,
            "association_type": association_type_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    association = one(actual_state_db_session.query(Tilknytning).all())
    assert sql_to_dict(association) == {
        "uuid": association_uuid,
        "bvn": "my_association",
        "bruger_uuid": person_uuid,
        "enhed_uuid": unit_uuid,
        "tilknytningstype_uuid": association_type_uuid,
        "tilknytningstype_titel": "Assoc Type",
        "stillingsbetegnelse_uuid": None,
        "stillingsbetegnelse_titel": None,
        "it_forbindelse_uuid": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
        "primær_boolean": None,
        "faglig_organisation": None,
    }
