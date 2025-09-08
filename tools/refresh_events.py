from typing import Any
from uuid import UUID

import click
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql

from tools.log import LogLevel
from tools.log import setup_logging

setup_logging(LogLevel("INFO"))


@click.command()
@click.option("--mora-base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option("--client-secret", envvar="CLIENT_SECRET")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option("--auth-server", envvar="AUTH_SERVER")
@click.option("--owner", type=click.UUID)
@click.option("--limit")
@click.argument(
    "objecttype",
    type=click.Choice(
        [
            "address",
            "association",
            "class",
            "employee",
            "engagement",
            "facet",
            "itsystem",
            "ituser",
            "kle",
            "leave",
            "manager",
            "org_unit",
            "owner",
            "related_unit",
            "rolebinding",
        ]
    ),
)
def refresh_events(
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    owner: UUID,
    limit: int,
    objecttype: str,
) -> None:
    """Post graphql refresh mutators paginating through all objects for the given object type"""
    
    mutator = f"""mutation MyMutation($cursor: Cursor = null, $limit: int = "1000", $owner: UUID!) {{
    {objecttype}_refresh(
        owner: $owner
        limit: $limit
        cursor: $cursor
        ) {{
        objects
          page_info {{
              next_cursor
          }}
        }}
    }}
    """
    count = 0
    with GraphQLClient(
        url=f"{mora_base}/graphql/v25",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,  # type: ignore
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        variable_values: dict[str, Any] = {"limit": limit, "owner": str(owner)}
        # First refresh
        r = session.execute(gql(mutator), variable_values=variable_values)
        cursor = r[f"{objecttype}_refresh"]["page_info"]["next_cursor"]
        count += len(r[f"{objecttype}_refresh"]["objects"])
        # Repeat until cursor is None
        while cursor:
            variable_values["cursor"] = cursor
            r = session.execute(gql(mutator), variable_values=variable_values)
            cursor = r[f"{objecttype}_refresh"]["page_info"]["next_cursor"]
            count += len(r[f"{objecttype}_refresh"]["objects"])

    click.echo(f"Refreshed {count} {objecttype} events.")


if __name__ == "__main__":
    refresh_events()
