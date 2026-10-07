# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import os
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Iterator
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastramqpi.pytest_plugin import run_server
from fastramqpi.pytest_plugin import run_test_client
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
from more_itertools import one
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import Session

from sql_export.sql_table_defs import Base
from sql_export.sql_table_defs import Enhed

from ..conftest import VALIDITY
from ..conftest import sql_to_dict


@pytest.fixture
def historic_state_db_session() -> Iterator[Session]:
    """Session for the historic state DB, truncated before the test."""
    db_user = os.environ["HISTORIC_STATE__USER"]
    db_pass = os.environ["HISTORIC_STATE__PASSWORD"]
    db_host = os.environ["HISTORIC_STATE__HOST"]
    db_port = os.environ.get("HISTORIC_STATE__PORT", "5432")
    db_name = os.environ["HISTORIC_STATE__DB_NAME"]

    url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(text(f"TRUNCATE TABLE {table.name} CASCADE"))
        session.commit()
        yield session


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
async def test_org_unit_actualstate_event(
    actual_state_db_session: Session,
    app: FastAPI,
    unit_type_class: str,
    level_class: str,
    parent_unit: str,
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
) -> None:
    """Create an org unit with a parent and call the /events/actualstate/org_unit and check that the org unit is exported correctly."""
    input_data = {
        "user_key": "my_unit",
        "name": "My Unit",
        "parent": parent_unit,
        "org_unit_type": unit_type_class,
        "org_unit_level": level_class,
        "validity": VALIDITY,
    }
    unit_uuid = await create_org_unit(input_data)

    # Start the app after creating the MO objects, so the classes are cached on startup
    async with run_server(app), run_test_client() as test_client:
        response = await test_client.post(
            "/events/actualstate/org_unit",
            json={"subject": unit_uuid, "priority": 0},
        )
        assert response.status_code == 200

    unit = one(actual_state_db_session.query(Enhed).filter_by(uuid=unit_uuid).all())
    assert sql_to_dict(unit) == {
        "uuid": unit_uuid,
        "navn": input_data["name"],
        "bvn": input_data["user_key"],
        "forældreenhed_uuid": parent_unit,
        "enhedstype_uuid": unit_type_class,
        "enhedstype_titel": "Unit Type",
        "enhedsniveau_uuid": level_class,
        "enhedsniveau_titel": "Level",
        "tidsregistrering_uuid": None,
        "tidsregistrering_titel": "",
        "organisatorisk_sti": "Parent Unit\\My Unit",
        "leder_uuid": None,
        "fungerende_leder_uuid": None,
        "opmærkning_uuid": None,
        "opmærkning_titel": None,
        "startdato": "2020-01-01",
        "slutdato": "9999-12-31",
    }


@pytest.mark.integration_test
async def test_org_unit_historic_event(
    historic_state_db_session: Session,
    app: FastAPI,
    graphql_client: GraphQLClient,
    unit_type_class: str,
    level_class: str,
    parent_unit: str,
    create_org_unit: Callable[[dict[str, Any]], Awaitable[str]],
) -> None:
    """Create an org unit with a parent and two validities and call the /events/historic/org_unit and check that both validities are exported."""
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

    # Start the app after creating the MO objects, so the classes are cached on startup
    async with run_server(app), run_test_client() as test_client:
        response = await test_client.post(
            "/events/historic/org_unit",
            json={"subject": unit_uuid, "priority": 0},
        )
        assert response.status_code == 200

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
        # The location and managers are not calculated for the historic export
        "organisatorisk_sti": None,
        "leder_uuid": None,
        "fungerende_leder_uuid": None,
        "opmærkning_uuid": None,
        "opmærkning_titel": None,
    }

    units = (
        historic_state_db_session.query(Enhed)
        .filter_by(uuid=unit_uuid)
        .order_by(Enhed.startdato)
        .all()
    )
    assert [sql_to_dict(unit) for unit in units] == [
        {
            **common,
            "navn": "My Unit",
            "startdato": "2020-01-01",
            "slutdato": "2020-12-31",
        },
        {
            **common,
            "navn": "Renamed Unit",
            "startdato": "2021-01-01",
            "slutdato": "9999-12-31",
        },
    ]
