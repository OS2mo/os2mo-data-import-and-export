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
from sql_export.sql_table_defs import DARAdresse

from .conftest import VALIDITY
from .conftest import sql_to_dict

# These two fit together
DAR_UUID = "0a3f50a0-23c9-32b8-e044-0003ba298018"
DAR_BETEGNELSE = "Pilestræde 43, 3., 1112 København K"


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

    await create_address(
        {
            "value": DAR_UUID,
            "person": person_uuid,
            "address_type": dar_address_type_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger()

    # Only betegnelse is populated from MO's GraphQL name field.
    # The remaining DAR fields (vejkode, vejnavn, etc.) have never been populated
    # since the old lora_cache.py (which called the DAR API) was replaced by
    # gql_lora_cache_async.py, which only stores betegnelse.
    dar_address = one(actual_state_db_session.query(DARAdresse).all())
    assert sql_to_dict(dar_address) == {
        "uuid": DAR_UUID,
        "vejkode": None,
        "vejnavn": None,
        "husnr": None,
        "etage": None,
        "dør": None,
        "postnr": None,
        "postnrnavn": None,
        "kommunekode": None,
        "adgangsadresseid": None,
        "betegnelse": DAR_BETEGNELSE,
    }

    # With resolve_dar=False, Adresse.værdi contains the raw DAR UUID
    addr = one(actual_state_db_session.query(Adresse).all())
    assert addr.værdi == DAR_UUID


@pytest.mark.integration_test
async def test_dar_address_sync_resolve_dar(
    trigger_with_dar: Callable[[], Awaitable[None]],
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

    await create_address(
        {
            "value": DAR_UUID,
            "person": person_uuid,
            "address_type": dar_address_type_uuid,
            "validity": VALIDITY,
        }
    )

    await trigger_with_dar()

    # Only betegnelse is populated from MO's GraphQL name field.
    # The remaining DAR fields (vejkode, vejnavn, etc.) have never been populated
    # since the old lora_cache.py (which called the DAR API) was replaced by
    # gql_lora_cache_async.py, which only stores betegnelse.
    dar_address = one(actual_state_db_session.query(DARAdresse).all())
    assert sql_to_dict(dar_address) == {
        "uuid": DAR_UUID,
        "vejkode": None,
        "vejnavn": None,
        "husnr": None,
        "etage": None,
        "dør": None,
        "postnr": None,
        "postnrnavn": None,
        "kommunekode": None,
        "adgangsadresseid": None,
        "betegnelse": DAR_BETEGNELSE,
    }

    # With resolve_dar=True, Adresse.værdi contains the resolved betegnelse
    # instead of the raw DAR UUID
    addr = one(actual_state_db_session.query(Adresse).all())
    assert addr.værdi == DAR_BETEGNELSE
