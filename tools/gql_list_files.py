import click
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql

from tools.log import LogLevel
from tools.log import setup_logging

setup_logging(LogLevel("INFO"))

GRAPHQL_VERSION = 25

list_files_query = gql("""
query ListFiles {
  files(filter: { file_store: EXPORTS }) {
    objects {
      file_name
    }
  }
}
""")


@click.command()
@click.option("--mora-base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option("--client-secret", envvar="CLIENT_SECRET")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option("--auth-server", envvar="AUTH_SERVER")
def list_files(
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
) -> None:
    """Read files from MOs filestore"""
    with GraphQLClient(
        url=f"{mora_base}/graphql/v{GRAPHQL_VERSION}",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,  # type: ignore
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        r = session.execute(list_files_query)

    click.echo(r["files"]["objects"])


if __name__ == "__main__":
    list_files()
