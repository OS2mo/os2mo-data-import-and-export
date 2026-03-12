# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import KLE

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_kle_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    kle_aspect_facet: UUID,
    kle_number_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_kle: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    kle_aspect_uuid = await create_class(
        {
            "user_key": "aspect",
            "name": "Aspect",
            "facet_uuid": str(kle_aspect_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    kle_number_uuid = await create_class(
        {
            "user_key": "number",
            "name": "Number",
            "facet_uuid": str(kle_number_facet),
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
    assert sql_to_dict(kle) == {
        "uuid": kle_uuid,
        "enhed_uuid": unit_uuid,
        "kle_aspekt_uuid": kle_aspect_uuid,
        "kle_aspekt_titel": "Aspect",
        "kle_nummer_uuid": kle_number_uuid,
        "kle_nummer_titel": "Number",
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
