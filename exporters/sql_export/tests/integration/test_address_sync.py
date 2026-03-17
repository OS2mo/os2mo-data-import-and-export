# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Adresse

from .conftest import VALIDITY
from .conftest import sql_to_dict


@pytest.mark.integration_test
async def test_address_sync(
    trigger: Callable[[], Awaitable[None]],
    address_type_facet: UUID,
    visibility_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_address: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Test address synchronization for a person (bruger_uuid)."""
    address_type_class = {
        "user_key": "email",
        "name": "Email",
        "facet_uuid": str(address_type_facet),
        "scope": "EMAIL",
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    address_type_uuid = await create_class(address_type_class)
    visibility_class = {
        "user_key": "public",
        "name": "Public",
        "facet_uuid": str(visibility_facet),
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    visibility_uuid = await create_class(visibility_class)

    person_uuid = await create_person(
        {
            "cpr_number": "0303700000",
            "given_name": "Addr",
            "surname": "User",
            "user_key": "addr_user",
        }
    )

    address_input = {
        "user_key": "my_address",
        "value": "test@example.com",
        "person": person_uuid,
        "address_type": address_type_uuid,
        "visibility": visibility_uuid,
        "validity": VALIDITY,
    }
    address_uuid = await create_address(address_input)

    await trigger()

    addr = one(actual_state_db_session.query(Adresse).all())
    assert sql_to_dict(addr) == {
        "uuid": address_uuid,
        "bvn": address_input["user_key"],
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "engagement_uuid": None,
        "ituser_uuid": None,
        "værdi": address_input["value"],
        "dar_uuid": None,
        "adressetype_uuid": address_type_uuid,
        "adressetype_bvn": address_type_class["user_key"],
        "adressetype_scope": "E-mail",
        "adressetype_titel": address_type_class["name"],
        "synlighed_uuid": visibility_uuid,
        "synlighed_scope": None,
        "synlighed_titel": visibility_class["name"],
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_org_unit_address_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    address_type_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_address: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Test address synchronization for an org unit (enhed_uuid)."""
    address_type_class = {
        "user_key": "phone",
        "name": "Phone",
        "facet_uuid": str(address_type_facet),
        "scope": "PHONE",
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    address_type_uuid = await create_class(address_type_class)
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
            "user_key": "addr_unit",
            "name": "Addr Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )

    address_input = {
        "user_key": "unit_phone",
        "value": "12345678",
        "org_unit": unit_uuid,
        "address_type": address_type_uuid,
        "validity": VALIDITY,
    }
    address_uuid = await create_address(address_input)

    await trigger()

    addr = one(actual_state_db_session.query(Adresse).all())
    assert sql_to_dict(addr) == {
        "uuid": address_uuid,
        "bvn": address_input["user_key"],
        "bruger_uuid": None,
        "enhed_uuid": unit_uuid,
        "engagement_uuid": None,
        "ituser_uuid": None,
        "værdi": address_input["value"],
        "dar_uuid": None,
        "adressetype_uuid": address_type_uuid,
        "adressetype_bvn": address_type_class["user_key"],
        "adressetype_scope": "Telefon",
        "adressetype_titel": address_type_class["name"],
        "synlighed_uuid": None,
        "synlighed_scope": None,
        "synlighed_titel": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_address_engagement_sync(
    trigger: Callable[[], Awaitable[None]],
    org_unit_type_facet: UUID,
    org_unit_level_facet: UUID,
    engagement_type_facet: UUID,
    job_function_facet: UUID,
    address_type_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    create_engagement: Callable[[dict[str, Any]], Awaitable[str]],
    create_address: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Test that address synchronization includes the engagement_uuid."""
    address_type_class = {
        "user_key": "email",
        "name": "Email",
        "facet_uuid": str(address_type_facet),
        "scope": "EMAIL",
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    address_type_uuid = await create_class(address_type_class)
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
            "cpr_number": "0303700000",
            "given_name": "Addr",
            "surname": "User",
            "user_key": "addr_user",
        }
    )
    unit_uuid = await create_org_unit(
        {
            "user_key": "addr_unit",
            "name": "Addr Unit",
            "org_unit_type": unit_type_uuid,
            "org_unit_level": level_uuid,
            "validity": VALIDITY,
        }
    )
    engagement_uuid = await create_engagement(
        {
            "user_key": "addr_eng",
            "person": person_uuid,
            "org_unit": unit_uuid,
            "engagement_type": engagement_type_uuid,
            "job_function": job_function_uuid,
            "validity": VALIDITY,
        }
    )

    address_input = {
        "user_key": "my_address",
        "value": "test@example.com",
        "person": person_uuid,
        "address_type": address_type_uuid,
        "engagement": engagement_uuid,
        "validity": VALIDITY,
    }
    address_uuid = await create_address(address_input)

    await trigger()

    addr = one(actual_state_db_session.query(Adresse).all())
    assert sql_to_dict(addr) == {
        "uuid": address_uuid,
        "bvn": address_input["user_key"],
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "engagement_uuid": address_input["engagement"],
        "ituser_uuid": None,
        "værdi": address_input["value"],
        "dar_uuid": None,
        "adressetype_uuid": address_type_uuid,
        "adressetype_bvn": address_type_class["user_key"],
        "adressetype_scope": "E-mail",
        "adressetype_titel": address_type_class["name"],
        "synlighed_uuid": None,
        "synlighed_scope": None,
        "synlighed_titel": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_address_ituser_sync(
    trigger: Callable[[], Awaitable[None]],
    address_type_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_system: Callable[[dict[str, Any]], Awaitable[str]],
    create_it_connection: Callable[[dict[str, Any]], Awaitable[str]],
    create_address: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    """Test that address synchronization includes the ituser_uuid."""
    address_type_class = {
        "user_key": "email",
        "name": "Email",
        "facet_uuid": str(address_type_facet),
        "scope": "EMAIL",
        "published": "Publiceret",
        "validity": VALIDITY,
    }
    address_type_uuid = await create_class(address_type_class)

    person_uuid = await create_person(
        {
            "cpr_number": "0303700000",
            "given_name": "Addr",
            "surname": "User",
            "user_key": "addr_user",
        }
    )

    it_system_uuid = await create_it_system(
        {
            "user_key": "ad",
            "name": "Active Directory",
            "validity": VALIDITY,
        }
    )

    ituser_uuid = await create_it_connection(
        {
            "user_key": "ad_user",
            "person": person_uuid,
            "itsystem": it_system_uuid,
            "validity": VALIDITY,
        }
    )

    address_input = {
        "user_key": "my_address",
        "value": "test@example.com",
        "person": person_uuid,
        "address_type": address_type_uuid,
        "ituser": ituser_uuid,
        "validity": VALIDITY,
    }
    address_uuid = await create_address(address_input)

    await trigger()

    addr = one(actual_state_db_session.query(Adresse).all())
    assert sql_to_dict(addr) == {
        "uuid": address_uuid,
        "bvn": address_input["user_key"],
        "bruger_uuid": person_uuid,
        "enhed_uuid": None,
        "engagement_uuid": None,
        "ituser_uuid": address_input["ituser"],
        "værdi": address_input["value"],
        "dar_uuid": None,
        "adressetype_uuid": address_type_uuid,
        "adressetype_bvn": address_type_class["user_key"],
        "adressetype_scope": "E-mail",
        "adressetype_titel": address_type_class["name"],
        "synlighed_uuid": None,
        "synlighed_scope": None,
        "synlighed_titel": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }
