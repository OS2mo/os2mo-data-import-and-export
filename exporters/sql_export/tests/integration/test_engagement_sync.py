# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from sql_export.sql_table_defs import Engagement
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_engagement_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_engagement: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    engagement_type_facet = await create_facet(
        {"user_key": "engagement_type", "published": "Publiceret", "validity": VALIDITY}
    )
    job_function_facet = await create_facet(
        {"user_key": "job_function", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "published": "Publiceret", "validity": VALIDITY}
    )

    engagement_type_uuid = await create_class(
        {
            "user_key": "eng_type",
            "name": "Eng Type",
            "facet_uuid": engagement_type_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    job_function_uuid = await create_class(
        {
            "user_key": "job_func",
            "name": "Job Func",
            "facet_uuid": job_function_facet,
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
            "cpr_number": "0202700000",
            "given_name": "Eng",
            "surname": "User",
            "user_key": "eng_user",
        }
    )
    unit_uuid = await create_org_unit(
        {
            "user_key": "eng_unit",
            "name": "Eng Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    engagement_uuid = await create_engagement(
        {
            "user_key": "my_eng",
            "person": person_uuid,
            "org_unit": unit_uuid,
            "engagement_type": engagement_type_uuid,
            "job_function": job_function_uuid,
            "fraction": 100,
            "validity": VALIDITY,
        }
    )

    await trigger()

    engagements = actual_state_db_session.query(Engagement).all()
    found_eng = next((e for e in engagements if e.uuid == engagement_uuid), None)

    assert found_eng is not None
    assert found_eng.bvn == "my_eng"
    assert found_eng.bruger_uuid == person_uuid
    assert found_eng.enhed_uuid == unit_uuid
    assert found_eng.engagementstype_uuid == engagement_type_uuid
    assert found_eng.stillingsbetegnelse_uuid == job_function_uuid
