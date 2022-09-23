import click
from gql import gql
from raclients.graph.client import GraphQLClient


@click.command()
@click.option("--mora_base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--client_id", envvar="CLIENT_ID", default="dipex")
@click.option("--client_secret", envvar="CLIENT_SECRET")
@click.option("--auth_realm", envvar="AUTH_REALM", default="mo")
@click.option("--auth_server", envvar="AUTH_SERVER")
@click.argument("query")
def gql_cli(
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    query,
) -> None:
    """Post graphql query to OS2MO.

    Example query: 'query MyQuery {employees { uuid }}'
    """
    q = gql(query)
    with GraphQLClient(
        url=f"{mora_base}/graphql/v2",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:

        r = session.execute(q)
    click.echo(r)


if __name__ == "__main__":
    gql_cli()
