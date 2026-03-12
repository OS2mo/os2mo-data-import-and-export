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

from sql_export.sql_table_defs import Enhed

from .conftest import VALIDITY
from .conftest import sql_to_dict


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
    assert sql_to_dict(unit) == {
        "id": ANY,
        "uuid": unit_uuid,
        "navn": input_data["name"],
        "bvn": input_data["user_key"],
        "forældreenhed_uuid": None,
        "enhedstype_uuid": unit_type_uuid,
        "enhedstype_titel": "Unit Type",
        "enhedsniveau_uuid": level_uuid,
        "enhedsniveau_titel": "Level",
        "tidsregistrering_uuid": None,
        "tidsregistrering_titel": "",
        "organisatorisk_sti": input_data["name"],
        "leder_uuid": None,
        "fungerende_leder_uuid": None,
        "opmærkning_uuid": None,
        "opmærkning_titel": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_org_unit_with_parent_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Test that a child unit has the correct parent UUID and organisatorisk_sti."""
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

    parent_uuid = await create_org_unit(
        {
            "user_key": "parent_unit",
            "name": "Parent Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )
    child_uuid = await create_org_unit(
        {
            "user_key": "child_unit",
            "name": "Child Unit",
            "parent": parent_uuid,
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    child = one(actual_state_db_session.query(Enhed).filter_by(uuid=child_uuid).all())
    assert sql_to_dict(child) == {
        "id": ANY,
        "uuid": child_uuid,
        "navn": "Child Unit",
        "bvn": "child_unit",
        "forældreenhed_uuid": parent_uuid,
        "enhedstype_uuid": unit_type_uuid,
        "enhedstype_titel": "Unit Type",
        "enhedsniveau_uuid": level_uuid,
        "enhedsniveau_titel": "Level",
        "tidsregistrering_uuid": None,
        "tidsregistrering_titel": "",
        "organisatorisk_sti": "Parent Unit\\Child Unit",
        "leder_uuid": None,
        "fungerende_leder_uuid": None,
        "opmærkning_uuid": None,
        "opmærkning_titel": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
