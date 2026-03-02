# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from more_itertools import one
from sql_export.sql_table_defs import DARAdresse
from sqlalchemy.orm import Session

from .conftest import VALIDITY


@pytest.mark.integration_test
async def test_dar_address_sync(
    trigger: Callable[[], Awaitable[None]],
    address_type_facet: UUID,
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    create_person: Callable[[dict[str, Any]], Awaitable[str]],
    create_address: Callable[[dict[str, Any]], Awaitable[str]],
    actual_state_db_session: Session,
) -> None:
    dar_address_type_uuid = await create_class(
        {
            "user_key": "dar_addr",
            "name": "DAR Address",
            "facet_uuid": str(address_type_facet),
            "scope": "DAR",
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )

    person_uuid = await create_person(
        {
            "cpr_number": "0808700000",
            "given_name": "DAR",
            "surname": "User",
            "user_key": "dar_user",
        }
    )

    dar_uuid = "0a3f50a0-23c9-32b8-e044-0003ba298018"
    await create_address(
        {
            "value": dar_uuid,
            "person": person_uuid,
            "address_type": dar_address_type_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    dar_address = one(actual_state_db_session.query(DARAdresse).all())
    assert dar_address.uuid == dar_uuid
    assert dar_address.betegnelse is not None
