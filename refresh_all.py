import asyncio
import logging
from typing import Literal

from gql import gql
from gql.client import AsyncClientSession
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient

logger = logging.getLogger(__name__)

object_types = [
    "engagement",
    "address",
    "association",
    "employee",
    "class",
    "facet",
    "ituser",
    "kle",
    "leave",
    "manager",
    "org_unit",
    "owner",
    "related_unit",
    "role",
]
refresh_types = Literal[
    "engagement",
    "address",
    "association",
    "employee",
    "class",
    "facet",
    "ituser",
    "kle",
    "leave",
    "manager",
    "org_unit",
    "owner",
    "related_unit",
    "role",
]


def create_query(refresh_type: refresh_types):
    return gql(
        f"""
        mutation RefreshMutation($limit: int, $cursor: Cursor) {{
        {refresh_type}_refresh(limit: $limit, cursor: $cursor, filter: {{from_date: null, to_date: null}}) {{
            objects
            page_info {{
            next_cursor
            }}
        }}
        }}
    """
    )


async def refresh_all_of_type(
    session: AsyncClientSession, refresh_type: refresh_types, limit: int = 100
):
    variable_values = {"cursor": None, "limit": limit}
    while True:
        res = await session.execute(
            create_query(refresh_type=refresh_type), variable_values=variable_values
        )
        yield res
        cursor = res[f"{refresh_type}_refresh"]["page_info"]["next_cursor"]
        if cursor is None:
            break
        variable_values["cursor"] = cursor


async def call_refresh_paged(session: AsyncClientSession, refresh_type: refresh_types):
    async for a in refresh_all_of_type(session=session, refresh_type=refresh_type):
        print(a)


async def refresh_all():
    settings = JobSettings()
    settings.start_logging_based_on_settings()

    async with GraphQLClient(
        url=f"{settings.mora_base}/graphql/v20",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_realm=settings.auth_realm,
        httpx_client_kwargs={"timeout": 300},
        execute_timeout=300,
        auth_server=settings.auth_server,
    ) as graphql_session:

        async with asyncio.TaskGroup() as tg:
            for o in object_types:
                tg.create_task(
                    call_refresh_paged(session=graphql_session, refresh_type=o)
                )


if __name__ == "__main__":
    asyncio.run(refresh_all())
