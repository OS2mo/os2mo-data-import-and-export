# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import ItForbindelse
from sql_export.sql_table_defs import ItForbindelseEngagement

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_it_connection_sync(
    trigger: Callable[[], Awaitable[None]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    person_uuid = await create_person(
        {
            "cpr_number": "0404700000",
            "given_name": "IT",
            "surname": "User",
            "user_key": "it_user",
        }
    )
    it_system_uuid = await create_it_system(
        {
            "name": "My System",
            "user_key": "my_system",
            "validity": VALIDITY,
        }
    )

    connection_input = {
        "user_key": "it_username",
        "person": person_uuid,
        "itsystem": it_system_uuid,
        "validity": VALIDITY,
    }
    it_connection_uuid = await create_it_connection(connection_input)

    await trigger()

    conn = one(actual_state_db_session.query(ItForbindelse).all())
    assert sql_to_dict(conn) == {
        "uuid": it_connection_uuid,
        "it_system_uuid": it_system_uuid,
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "brugernavn": connection_input["user_key"],
        "eksternt_id": None,
        "primær_boolean": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_it_connection_external_id_sync(
    trigger: Callable[[], Awaitable[None]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    person_uuid = await create_person(
        {
            "cpr_number": "0404700000",
            "given_name": "IT",
            "surname": "User",
            "user_key": "it_user",
        }
    )
    it_system_uuid = await create_it_system(
        {
            "name": "My System",
            "user_key": "my_system",
            "validity": VALIDITY,
        }
    )

    connection_input = {
        "user_key": "it_username",
        "person": person_uuid,
        "itsystem": it_system_uuid,
        "external_id": "ext-12345",
        "validity": VALIDITY,
    }
    it_connection_uuid = await create_it_connection(connection_input)

    await trigger()

    conn = one(actual_state_db_session.query(ItForbindelse).all())
    assert sql_to_dict(conn) == {
        "uuid": it_connection_uuid,
        "it_system_uuid": it_system_uuid,
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "brugernavn": connection_input["user_key"],
        "eksternt_id": connection_input["external_id"],
        "primær_boolean": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_it_connection_engagement_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    engagement_type_facet: UUID,
    job_function_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_engagement: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    engagement_type_uuid = await create_class(
        {
            "user_key": "eng_type",
            "name": "Eng Type",
            "facet_uuid": str(engagement_type_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    job_function_uuid = await create_class(
        {
            "user_key": "job_func",
            "name": "Job Func",
            "facet_uuid": str(job_function_facet),
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
            "cpr_number": "0404700000",
            "given_name": "IT",
            "surname": "User",
            "user_key": "it_user",
        }
    )
    unit_uuid = await create_org_unit(
        {
            "user_key": "it_unit",
            "name": "IT Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )
    engagement_uuid_1 = await create_engagement(
        {
            "user_key": "it_eng_1",
            "person": person_uuid,
            "org_unit": unit_uuid,
            "engagement_type": engagement_type_uuid,
            "job_function": job_function_uuid,
            "validity": VALIDITY,
        }
    )
    engagement_uuid_2 = await create_engagement(
        {
            "user_key": "it_eng_2",
            "person": person_uuid,
            "org_unit": unit_uuid,
            "engagement_type": engagement_type_uuid,
            "job_function": job_function_uuid,
            "validity": VALIDITY,
        }
    )

    it_system_uuid = await create_it_system(
        {
            "name": "My System",
            "user_key": "my_system",
            "validity": VALIDITY,
        }
    )

    connection_input = {
        "user_key": "it_username",
        "person": person_uuid,
        "itsystem": it_system_uuid,
        "engagements": [engagement_uuid_1, engagement_uuid_2],
        "validity": VALIDITY,
    }
    it_connection_uuid = await create_it_connection(connection_input)

    await trigger()

    conn = one(actual_state_db_session.query(ItForbindelse).all())
    assert sql_to_dict(conn) == {
        "uuid": it_connection_uuid,
        "it_system_uuid": it_system_uuid,
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "brugernavn": connection_input["user_key"],
        "eksternt_id": None,
        "primær_boolean": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }

    engagement_links = actual_state_db_session.query(ItForbindelseEngagement).all()
    assert len(engagement_links) == 2
    links_by_engagement = {
        sql_to_dict(link)["engagement_uuid"]: sql_to_dict(link)
        for link in engagement_links
    }
    assert links_by_engagement[engagement_uuid_1] == {
        "it_forbindelse_uuid": it_connection_uuid,
        "engagement_uuid": engagement_uuid_1,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
    assert links_by_engagement[engagement_uuid_2] == {
        "it_forbindelse_uuid": it_connection_uuid,
        "engagement_uuid": engagement_uuid_2,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
