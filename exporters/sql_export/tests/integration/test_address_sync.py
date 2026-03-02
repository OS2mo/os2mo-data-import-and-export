# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from sql_export.sql_table_defs import Adresse
from sqlalchemy.orm import Session

from .conftest import VALIDITY


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
