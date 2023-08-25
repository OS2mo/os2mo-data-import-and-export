import logging
from uuid import UUID

import click
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2sync import get_hierarchy_raw
from os2sync_export.os2sync import get_os2sync_session
from os2sync_export.os2sync import trigger_hierarchy


logger = logging.getLogger(__name__)


def get_hierarchy_users(settings, client):
    request_uuid = trigger_hierarchy(client, os2sync_api_url=settings.os2sync_api_url)
    response = get_hierarchy_raw(
        client,
        os2sync_api_url=settings.os2sync_api_url,
        request_uuid=request_uuid,
    )
    return response["Users"]


def get_user_uuids_to_fix(hierarchy_users: list[dict]) -> list[UUID]:
    def has_duplicated_positions(item: dict):
        # `item` must have more than one position
        if len(item.get("Positions", [])) < 1:
            return False
        # Detect duplicated positions
        positions: list[tuple] = [(p["Uuid"], p["Name"]) for p in item["Positions"]]
        return len(positions) > len(set(positions))

    return [
        UUID(item["Uuid"]) for item in hierarchy_users if has_duplicated_positions(item)
    ]


def _remove_from_fkorg(settings, client, user_uuids: list[UUID]) -> None:
    for user_uuid in user_uuids:
        url = f"{settings.os2sync_api_url}/user/{user_uuid}"
        try:
            client.delete(url)
        except Exception:
            logger.exception("could not remove user %r (url=%r)", user_uuid, url)
        else:
            click.echo(f"Removed user {user_uuid} from FK ORG")


@click.group(
    help="""
        Intended usage:

        1. List affected users:
        $ python -m os2sync_export.fix_fkorg_duplicated_positions find

        2. Remove affected users from FK ORG:
        $ python -m os2sync_export.fix_fkorg_duplicated_positions remove_from_fkorg

        3. Remove all entries in OS2sync `success_users` MySQL table.

        4. Trigger a full re-export of all MO data to OS2sync.
    """
)
@click.pass_context
def cli(ctx) -> None:
    ctx.ensure_object(dict)

    settings = get_os2sync_settings()
    settings.start_logging_based_on_settings()
    ctx.obj["settings"] = settings

    client = get_os2sync_session()
    ctx.obj["client"] = client

    hierarchy_users = get_hierarchy_users(settings, client)
    user_uuids_to_fix = get_user_uuids_to_fix(hierarchy_users)
    ctx.obj["user_uuids_to_fix"] = user_uuids_to_fix


@cli.command(help="List UUIDs of FK ORG users that have duplicated positions")
@click.pass_context
def find(ctx):
    click.echo("Listing UUIDs of FK ORG users that have duplicated positions:")
    for user_uuid in ctx.obj["user_uuids_to_fix"]:
        click.echo(user_uuid)


@cli.command(help="Remove users from FK ORG if they have duplicated positions")
@click.pass_context
def remove_from_fkorg(ctx):
    click.echo("Removing users with duplicated positions from FK ORG ...")
    _remove_from_fkorg(
        ctx.obj["settings"], ctx.obj["client"], ctx.obj["user_uuids_to_fix"]
    )


if __name__ == "__main__":
    cli(obj={})
