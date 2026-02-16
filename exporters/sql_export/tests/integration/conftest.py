import os
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Iterator

import pytest
from fastapi import FastAPI
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
from gql.client import AsyncClientSession
from httpx import AsyncClient
from pydantic import AnyHttpUrl
from pydantic import parse_obj_as
from sql_export.main import create_app
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

GRAPHQL_VERSION = 22


@pytest.fixture
def app(load_marked_envvars: None) -> FastAPI:
    # Ensure EVENTDRIVEN is False so /trigger endpoint is mounted
    os.environ["EVENTDRIVEN"] = "false"
    return create_app()


@pytest.fixture
async def graphql_client(mo_client: AsyncClient) -> AsyncIterator[AsyncClientSession]:
    """Authenticated GraphQL codegen client for OS2mo."""
    url = f"{mo_client.base_url}/graphql/v{GRAPHQL_VERSION}"

    client = GraphQLClient(
        url=url,
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        auth_realm=os.environ["AUTH_REALM"],
        auth_server=parse_obj_as(AnyHttpUrl, os.environ["AUTH_SERVER"]),
    )
    async with client as c:
        yield c


@pytest.fixture
def actual_state_db_session() -> Iterator[Session]:
    # Connect to the actual state DB
    db_user = os.environ["ACTUAL_STATE__USER"]
    db_pass = os.environ["ACTUAL_STATE__PASSWORD"]
    db_host = os.environ["ACTUAL_STATE__HOST"]
    db_port = os.environ.get("ACTUAL_STATE__PORT", "5432")
    db_name = os.environ["ACTUAL_STATE__DB_NAME"]

    url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)
    with Session(engine) as session:
        yield session


@pytest.fixture
def create_person(
    graphql_client: AsyncClientSession,
) -> Callable[[dict[str, Any]], Awaitable[str]]:
    """Returns a function to create a Person."""

    async def inner(input_data: dict[str, Any]) -> str:
        create_mutation = gql("""
        mutation CreateEmployee($input: EmployeeCreateInput!) {
            employee_create(input: $input) {
                uuid
            }
        }
        """)

        create_resp = await graphql_client.execute(
            create_mutation, variable_values={"input": input_data}
        )
        return create_resp["employee_create"]["uuid"]

    return inner
