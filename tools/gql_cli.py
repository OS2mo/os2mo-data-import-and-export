from json import dumps

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
@click.option("--graphql-version", envvar="GRAPHQL_VERSION", default="20", type=int)
@click.option("--json", envvar="OUTPUT_JSON", default=False, type=bool, is_flag=True)
@click.argument("query")
def gql_cli(
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    graphql_version: int,
    json: bool,
    query,
) -> None:
    """Post graphql query to OS2MO.

    Example query: 'query MyQuery {employees { uuid }}'
    """
    q = gql(query)
    with GraphQLClient(
        url=f"{mora_base}/graphql/v{graphql_version}",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        r = session.execute(q)

    if json:
        r = dumps(r, indent=2)

    click.echo(r)


if __name__ == "__main__":
    gql_cli()
