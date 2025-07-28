import click
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
from more_itertools import one

from tools.log import LogLevel
from tools.log import setup_logging

setup_logging(LogLevel("INFO"))

GRAPHQL_VERSION = 25


@click.command()
@click.option("--mora-base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option("--client-secret", envvar="CLIENT_SECRET")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option("--auth-server", envvar="AUTH_SERVER")
@click.argument("file_name", required=True)
@click.option("--base64", is_flag=True)
def read_file(
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    file_name,
    base64,
) -> None:
    """Read files from MOs filestore"""
    contents_type = "base64_contents" if base64 else "text_contents"
    query = gql(f"""
    query ReadFile($file_name: String!) {{
    files(filter: {{ file_store: EXPORTS, file_names: [$file_name] }}) {{
        objects {{
        {contents_type}
        }}
    }}
    }}

    """)
    with GraphQLClient(
        url=f"{mora_base}/graphql/v{GRAPHQL_VERSION}",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,  # type: ignore
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        r = session.execute(query, variable_values={"file_name": file_name})
    file_contents = one(r["files"]["objects"])
    click.echo(file_contents[contents_type])


if __name__ == "__main__":
    read_file()
