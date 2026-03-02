# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from httpx import AsyncClient
from more_itertools import one
from sql_export.sql_table_defs import Bruger
from sql_export.sql_table_defs import Enhed
from sql_export.sql_table_defs import Engagement
from sql_export.sql_table_defs import Adresse
from sql_export.sql_table_defs import ItSystem
from sql_export.sql_table_defs import ItForbindelse
from sql_export.sql_table_defs import Leder
from sql_export.sql_table_defs import LederAnsvar
from sqlalchemy.orm import Session

VALIDITY = {"from": "2020-01-01", "to": None}


@pytest.mark.integration_test
def test_trigger1() -> None:
    pass


@pytest.fixture
def trigger(test_client: AsyncClient) -> Callable[[], Awaitable[None]]:
    async def inner() -> None:
        response = await test_client.post(
            "/trigger",
            params={
                "resolve_dar": False,
                "historic": False,
                "read_from_cache": False,
            },
        )
        assert response.status_code == 200
        assert response.json() == {"detail": "Triggered"}

        response = await test_client.post(
            "/wait_for_finish",
            params={"historic": False},
            timeout=60.0,
        )
        assert response.status_code == 200
        assert response.json() == {"detail": "Finished"}

    return inner


@pytest.mark.integration_test
async def test_employee_sync(
    trigger: Callable[[], Awaitable[None]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    input_data = {
        "cpr_number": "0101700000",
        "given_name": "given_name",
        "surname": "surname",
        "user_key": "user_key",
    }
    person_uuid = await create_person(input_data)

    await trigger()

    users = actual_state_db_session.query(Bruger).all()
    user = one(users)
    assert user.uuid == person_uuid
    assert user.fornavn == input_data["given_name"]
    assert user.efternavn == input_data["surname"]
    assert user.cpr == input_data["cpr_number"]
    assert user.bvn == input_data["user_key"]


@pytest.mark.integration_test
async def test_org_unit_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "published": "Publiceret", "validity": VALIDITY}
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
    assert unit.uuid == unit_uuid
    assert unit.bvn == input_data["user_key"]
    assert unit.navn == input_data["name"]
    assert unit.enhedstype_uuid == unit_type_uuid
    assert unit.enhedsniveau_uuid == level_uuid


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


@pytest.mark.integration_test
async def test_address_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_address: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    address_type_facet = await create_facet(
        {"user_key": "address_type", "published": "Publiceret", "validity": VALIDITY}
    )
    visibility_facet = await create_facet(
        {"user_key": "visibility", "published": "Publiceret", "validity": VALIDITY}
    )

    address_type_uuid = await create_class(
        {
            "user_key": "email",
            "name": "Email",
            "facet_uuid": address_type_facet,
            "scope": "EMAIL",
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    visibility_uuid = await create_class(
        {
            "user_key": "public",
            "name": "Public",
            "facet_uuid": visibility_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )

    person_uuid = await create_person(
        {
            "cpr_number": "0303700000",
            "given_name": "Addr",
            "surname": "User",
            "user_key": "addr_user",
        }
    )

    address_uuid = await create_address(
        {
            "value": "test@example.com",
            "person": person_uuid,
            "address_type": address_type_uuid,
            "visibility": visibility_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    addresses = actual_state_db_session.query(Adresse).all()
    found_addr = next((a for a in addresses if a.uuid == address_uuid), None)

    assert found_addr is not None
    assert found_addr.værdi == "test@example.com"
    assert found_addr.bruger_uuid == person_uuid
    assert found_addr.adressetype_uuid == address_type_uuid
    assert found_addr.synlighed_uuid == visibility_uuid


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

    it_connection_uuid = await create_it_connection(
        {
            "user_key": "it_username",
            "person": person_uuid,
            "itsystem": it_system_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    it_systems = actual_state_db_session.query(ItSystem).all()
    found_sys = next((s for s in it_systems if s.uuid == it_system_uuid), None)
    assert found_sys is not None
    assert found_sys.navn == "My System"

    it_connections = actual_state_db_session.query(ItForbindelse).all()
    found_conn = next((c for c in it_connections if c.uuid == it_connection_uuid), None)

    assert found_conn is not None
    assert found_conn.it_system_uuid == it_system_uuid
    assert found_conn.bruger_uuid == person_uuid
    assert found_conn.brugernavn == "it_username"


@pytest.mark.integration_test
async def test_manager_sync(
    trigger: Callable[[], Awaitable[None]],
    create_facet: Callable[[dict[str, Any]], Awaitable[str]],
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_manager: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    manager_type_facet = await create_facet(
        {"user_key": "manager_type", "published": "Publiceret", "validity": VALIDITY}
    )
    manager_level_facet = await create_facet(
        {"user_key": "manager_level", "published": "Publiceret", "validity": VALIDITY}
    )
    responsibility_facet = await create_facet(
        {"user_key": "responsibility", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "published": "Publiceret", "validity": VALIDITY}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "published": "Publiceret", "validity": VALIDITY}
    )

    manager_type_uuid = await create_class(
        {
            "user_key": "leader",
            "name": "Leader",
            "facet_uuid": manager_type_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    manager_level_uuid = await create_class(
        {
            "user_key": "level1",
            "name": "Level 1",
            "facet_uuid": manager_level_facet,
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    responsibility_uuid = await create_class(
        {
            "user_key": "resp1",
            "name": "Responsibility 1",
            "facet_uuid": responsibility_facet,
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

    managers = actual_state_db_session.query(Leder).all()
    found_mgr = next((m for m in managers if m.uuid == manager_uuid), None)

    assert found_mgr is not None
    assert found_mgr.bruger_uuid == person_uuid
    assert found_mgr.enhed_uuid == unit_uuid
    assert found_mgr.ledertype_uuid == manager_type_uuid
    assert found_mgr.niveautype_uuid == manager_level_uuid

    responsibility = one(actual_state_db_session.query(LederAnsvar).filter_by(leder_uuid=manager_uuid).all())
    assert responsibility.lederansvar_uuid == responsibility_uuid
