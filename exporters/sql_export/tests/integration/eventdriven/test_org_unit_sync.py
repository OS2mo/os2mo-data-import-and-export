# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastramqpi.pytest_plugin import run_server
from fastramqpi.pytest_plugin import run_test_client
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
from more_itertools import one
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Enhed

from ..conftest import VALIDITY
from ..conftest import sql_to_dict
from .conftest import trigger_actualstate_sync
from .conftest import trigger_historic_sync


@pytest.fixture
async def unit_type_class(
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    org_unit_type_facet: UUID,
) -> str:
    return await create_class(
        {
            "user_key": "unit_type",
            "name": "Unit Type",
            "facet_uuid": str(org_unit_type_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )


@pytest.fixture
async def level_class(
    create_class: Callable[[dict[str, Any]], Awaitable[str]],
    org_unit_level_facet: UUID,
) -> str:
    return await create_class(
        {
            "user_key": "level",
            "name": "Level",
            "facet_uuid": str(org_unit_level_facet),
            "published": "Publiceret",
            "validity": VALIDITY,
        }
    )


@pytest.fixture
async def parent_unit(
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
    unit_type_class: str,
    level_class: str,
) -> str:
    return await create_org_unit(
        {
            "user_key": "parent_unit",
            "name": "Parent Unit",
            "org_unit_type": unit_type_class,
            "org_unit_level": level_class,
            "validity": VALIDITY,
        }
    )


@pytest.mark.integration_test
async def test_org_unit_event(
    actual_state_db_session: Session,
    historic_state_db_session: Session,
    app: FastAPI,
    graphql_client: GraphQLClient,
    unit_type_class: str,
    level_class: str,
    parent_unit: str,
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
) -> None:
    """Create an org unit with a parent and two validities and call the /events/{actualstate,historic}/org_unit routes.

    Check that the current validity is exported to actual state and both validities to historic.
    """
    # Arrange
    input_data = {
        "user_key": "my_unit",
        "name": "My Unit",
        "parent": parent_unit,
        "org_unit_type": unit_type_class,
        "org_unit_level": level_class,
        "validity": VALIDITY,
    }
    unit_uuid = await create_org_unit(input_data)
    # Rename the unit to get a second validity
    await graphql_client.execute(  # type: ignore[misc]
        gql("""
        mutation UpdateOrgUnit($input: OrganisationUnitUpdateInput!) {
            org_unit_update(input: $input) {
                uuid
            }
        }
        """),
        variable_values={
            "input": {
                "uuid": unit_uuid,
                "name": "Renamed Unit",
                "validity": {"from": "2021-01-01", "to": None},
            }
        },
    )

    # Act
    async with run_server(app), run_test_client() as test_client:
        await trigger_actualstate_sync(test_client, "org_unit", unit_uuid)
        await trigger_historic_sync(test_client, "org_unit", unit_uuid)

    # Assert
    common = {
        "uuid": unit_uuid,
        "bvn": input_data["user_key"],
        "forældreenhed_uuid": parent_unit,
        "enhedstype_uuid": unit_type_class,
        "enhedstype_titel": "Unit Type",
        "enhedsniveau_uuid": level_class,
        "enhedsniveau_titel": "Level",
        "tidsregistrering_uuid": None,
        "tidsregistrering_titel": "",
        "leder_uuid": None,
        "fungerende_leder_uuid": None,
        "opmærkning_uuid": None,
        "opmærkning_titel": None,
    }

    unit = one(actual_state_db_session.query(Enhed).filter_by(uuid=unit_uuid).all())
    assert sql_to_dict(unit) == {
        **common,
        "navn": "Renamed Unit",
        "organisatorisk_sti": "Parent Unit\\Renamed Unit",
        "startdato": "2021-01-01",
        "slutdato": "9999-12-31",
    }

    units = (
        historic_state_db_session.query(Enhed)
        .filter_by(uuid=unit_uuid)
        .order_by(Enhed.startdato)
        .all()
    )
    # The location is not calculated for the historic export
    assert [sql_to_dict(unit) for unit in units] == [
        {
            **common,
            "navn": "My Unit",
            "organisatorisk_sti": None,
            "startdato": "2020-01-01",
            "slutdato": "2020-12-31",
        },
        {
            **common,
            "navn": "Renamed Unit",
            "organisatorisk_sti": None,
            "startdato": "2021-01-01",
            "slutdato": "9999-12-31",
        },
    ]
