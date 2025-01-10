import click
import httpx
from gql import gql
from more_itertools import only
from fastramqpi.raclients.graph.client import GraphQLClient
from fastramqpi.raclients.graph.client import SyncClientSession

from tools.data_fixers.class_tools import delete_class
from tools.data_fixers.class_tools import move_class_helper


def get_class_uuid(session: SyncClientSession, facet_user_key, user_key):
    q = gql(
        """
    query ClassQuery($facet_user_keys: [String!], $user_keys:[String!])
        {
            classes(facet_user_keys: $facet_user_keys, user_keys: $user_keys)
            {
                uuid
            }
        }
    """
    )
    res = session.execute(
        q,
        variable_values={"facet_user_keys": [facet_user_key], "user_keys": [user_key]},
    )
    classes: dict = only(res["classes"], {})
    return classes.get("uuid")


@click.command()
@click.option("--remove", prompt=True, help="User-key of the class to remove")
@click.option("--keep", prompt=True, help="User-key of the class to keep")
@click.option(
    "--facet", prompt=True, help="User-key of the facet to which the classes belong"
)
@click.option("--mora_base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--mox_base", envvar="MOX_BASE", default="http://localhost:5000/lora")
@click.option("--client_id", envvar="CLIENT_ID", default="dipex")
@click.option("--client_secret", envvar="CLIENT_SECRET")
@click.option("--auth_realm", envvar="AUTH_REALM", default="mo")
@click.option(
    "--auth_server", envvar="AUTH_SERVER", default="http://localhost:5000/auth"
)
@click.option("--dry-run", is_flag=True)
def merge_classes(
    facet: str,
    remove: str,
    keep: str,
    mora_base: str,
    mox_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    dry_run: bool,
) -> None:
    """Helper tool to merge two classes into one.

    Moves any object from the removed class to the kept class before deleting the removed class.

    """

    with GraphQLClient(
        url=f"{mora_base}/graphql/v3",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        keep_uuid = get_class_uuid(session=session, facet_user_key=facet, user_key=keep)
        if keep_uuid is None:
            click.echo(f"No class with user-key {keep} found in facet {facet}")
            return
        remove_uuid = get_class_uuid(
            session=session, facet_user_key=facet, user_key=remove
        )
        if remove_uuid is None:
            click.echo(f"No class with user-key {keep} found in facet {facet}")
            return

    if dry_run:
        click.echo(f"Keeping class with uuid {keep_uuid}")
        click.echo(f"Removing class with uuid {remove_uuid}")
        return

    move_class_helper(
        old_uuid=remove_uuid,
        new_uuid=keep_uuid,
        copy=False,
        mox_base=mox_base,
    )
    delete_class(session=httpx.Client(), base=mox_base, uuid=remove_uuid)


if __name__ == "__main__":
    merge_classes()
