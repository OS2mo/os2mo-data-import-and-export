# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sql_export.sql_table_defs import Adresse
from sqlalchemy.orm import Session

from .conftest import VALIDITY


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
    address_type_uuid = await create_class(
        {
            "user_key": "email",
            "name": "Email",
            "facet_uuid": str(address_type_facet),
            "scope": "EMAIL",
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )
    visibility_uuid = await create_class(
        {
            "user_key": "public",
            "name": "Public",
            "facet_uuid": str(visibility_facet),
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

    addr = one(actual_state_db_session.query(Adresse).filter_by(uuid=address_uuid).all())
    assert addr.værdi == "test@example.com"
    assert addr.bruger_uuid == person_uuid
    assert addr.adressetype_uuid == address_type_uuid
    assert addr.synlighed_uuid == visibility_uuid
