# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from more_itertools import one
from sql_export.sql_table_defs import KLE
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_kle_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_kle: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    kle_aspect_facet = await create_facet(
        {"user_key": "kle_aspect", "published": "Publiceret", "validity": VALIDITY}
    )
    kle_number_facet = await create_facet(
        {"user_key": "kle_number", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "published": "Publiceret", "validity": VALIDITY}
    )

    kle_aspect_uuid = await create_class(
        {
            "user_key": "aspect",
            "name": "Aspect",
            "facet_uuid": kle_aspect_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    kle_number_uuid = await create_class(
        {
            "user_key": "number",
            "name": "Number",
            "facet_uuid": kle_number_facet,
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

    unit_uuid = await create_org_unit(
        {
            "user_key": "kle_unit",
            "name": "KLE Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    kle_uuid = await create_kle(
        {
            "org_unit": unit_uuid,
            "kle_aspects": [kle_aspect_uuid],
            "kle_number": kle_number_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    kle = one(actual_state_db_session.query(KLE).all())
    assert kle.uuid == kle_uuid
    assert kle.enhed_uuid == unit_uuid
    assert kle.kle_aspekt_uuid == kle_aspect_uuid
    assert kle.kle_nummer_uuid == kle_number_uuid
