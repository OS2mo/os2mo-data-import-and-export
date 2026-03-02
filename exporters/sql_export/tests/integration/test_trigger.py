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
from sqlalchemy.orm import Session


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

    # Read all users and assert there is one and only one
    users = actual_state_db_session.query(Bruger).all()
    user = one(users)
    # Assert that the read user has the expected data
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
    # 1. Create needed classes
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "description": "org_unit_type"}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "description": "org_unit_level"}
    )
    
    unit_type_uuid = await create_class(
        {
            "user_key": "unit_type",
            "title": "Unit Type",
            "facet_uuid": org_unit_type_facet,
            "scope": "TEXT",
        }
    )
    level_uuid = await create_class(
        {
            "user_key": "level",
            "title": "Level",
            "facet_uuid": org_unit_level_facet,
            "scope": "TEXT",
        }
    )

    # 2. Create the Org Unit
    input_data = {
        "user_key": "my_unit",
        "name": "My Unit",
        "org_unit_type_uuid": unit_type_uuid,
        "org_unit_level_uuid": level_uuid,
        "parent_uuid": None, # Root unit
        "validity": {"from": "2020-01-01", "to": None},
    }
    unit_uuid = await create_org_unit(input_data)

    await trigger()

    # 3. Read from DB and assert
    units = actual_state_db_session.query(Enhed).all()
    # Filter for our unit, assuming clean DB or at least check if ours is there
    # But integration tests usually clean up or run in isolation.
    # The fixture `purge_export_db` in conftest suggests cleanup.
    
    # We might have other units (like root unit created by system), so find ours
    found_unit = None
    for u in units:
        if u.uuid == unit_uuid:
            found_unit = u
            break
            
    assert found_unit is not None
    assert found_unit.bvn == input_data["user_key"]
    assert found_unit.navn == input_data["name"]
    assert found_unit.enhedstype_uuid == unit_type_uuid
    assert found_unit.enhedsniveau_uuid == level_uuid


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
    # 1. Setup dependencies
    engagement_type_facet = await create_facet(
        {"user_key": "engagement_type", "description": "engagement_type"}
    )
    job_function_facet = await create_facet(
        {"user_key": "job_function", "description": "job_function"}
    )
    org_unit_type_facet = await create_facet(
        {"user_key": "org_unit_type", "description": "org_unit_type"}
    )
    org_unit_level_facet = await create_facet(
        {"user_key": "org_unit_level", "description": "org_unit_level"}
    )

    engagement_type_uuid = await create_class(
        {
            "user_key": "eng_type",
            "title": "Eng Type",
            "facet_uuid": engagement_type_facet,
            "scope": "TEXT",
        }
    )
    job_function_uuid = await create_class(
        {
            "user_key": "job_func",
            "title": "Job Func",
            "facet_uuid": job_function_facet,
            "scope": "TEXT",
        }
    )
    unit_type_uuid = await create_class(
        {
            "user_key": "unit_type",
            "title": "Unit Type",
            "facet_uuid": org_unit_type_facet,
            "scope": "TEXT",
        }
    )
    level_uuid = await create_class(
        {
            "user_key": "level",
            "title": "Level",
            "facet_uuid": org_unit_level_facet,
            "scope": "TEXT",
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
            "org_unit_type_uuid": unit_type_uuid,
            "org_unit_level_uuid": level_uuid,
            "parent_uuid": None,
            "validity": {"from": "2020-01-01", "to": None},
        }
    )

    # 2. Create Engagement
    input_data = {
        "user_key": "my_eng",
        "employee_uuid": person_uuid,
        "org_unit_uuid": unit_uuid,
        "engagement_type_uuid": engagement_type_uuid,
        "job_function_uuid": job_function_uuid,
        "fraction": 100,
        "validity": {"from": "2020-01-01", "to": None},
    }
    engagement_uuid = await create_engagement(input_data)

    await trigger()

    # 3. Assert
    engagements = actual_state_db_session.query(Engagement).all()
    found_eng = next((e for e in engagements if e.uuid == engagement_uuid), None)
    
    assert found_eng is not None
    assert found_eng.bvn == input_data["user_key"]
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
    # 1. Setup dependencies
    address_type_facet = await create_facet(
        {"user_key": "address_type", "description": "address_type"}
    )
    visibility_facet = await create_facet(
        {"user_key": "visibility", "description": "visibility"}
    )

    address_type_uuid = await create_class(
        {
            "user_key": "email",
            "title": "Email",
            "facet_uuid": address_type_facet,
            "scope": "EMAIL",
        }
    )
    visibility_uuid = await create_class(
        {
            "user_key": "public",
            "title": "Public",
            "facet_uuid": visibility_facet,
            "scope": "TEXT",
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

    # 2. Create Address
    input_data = {
        "value": "test@example.com",
        "employee_uuid": person_uuid,
        "address_type_uuid": address_type_uuid,
        "visibility_uuid": visibility_uuid,
        "validity": {"from": "2020-01-01", "to": None},
    }
    address_uuid = await create_address(input_data)

    await trigger()

    # 3. Assert
    addresses = actual_state_db_session.query(Adresse).all()
    found_addr = next((a for a in addresses if a.uuid == address_uuid), None)
    
    assert found_addr is not None
    assert found_addr.værdi == input_data["value"]
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
    # 1. Setup dependencies
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
        }
    )

    # 2. Create IT Connection
    input_data = {
        "user_key": "it_username",
        "employee_uuid": person_uuid,
        "itsystem_uuid": it_system_uuid,
        "validity": {"from": "2020-01-01", "to": None},
    }
    it_connection_uuid = await create_it_connection(input_data)

    await trigger()

    # 3. Assert
    # Check ItSystem
    it_systems = actual_state_db_session.query(ItSystem).all()
    found_sys = next((s for s in it_systems if s.uuid == it_system_uuid), None)
    assert found_sys is not None
    assert found_sys.navn == "My System"

    # Check ItForbindelse
    it_connections = actual_state_db_session.query(ItForbindelse).all()
    found_conn = next((c for c in it_connections if c.uuid == it_connection_uuid), None)
    
    assert found_conn is not None
    assert found_conn.it_system_uuid == it_system_uuid
    assert found_conn.bruger_uuid == person_uuid
    assert found_conn.brugernavn == input_data["user_key"]
