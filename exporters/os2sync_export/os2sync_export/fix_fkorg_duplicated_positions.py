import logging
from uuid import UUID

import click
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2sync import get_hierarchy_raw
from os2sync_export.os2sync import get_os2sync_session
from os2sync_export.os2sync import trigger_hierarchy
from os2sync_export.os2synccli import update_single_user


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


def _remove_from_os2sync(user_uuids: list[UUID]) -> str:
    # Emit SQL which removes the given user UUIDs from the `success_users` table
    user_uuids_sql = ", ".join(["'%s'" % uuid for uuid in user_uuids])
    delete_sql: str = f"delete from success_users where uuid in ({user_uuids_sql})"
    return delete_sql


def _redo_export(settings, user_uuids: list[UUID], dry_run: bool = True):
    for user_uuid in user_uuids:
        try:
            update_single_user(user_uuid, settings, dry_run)
        except Exception:
            logger.exception("could not re-export user %r", user_uuid)
        else:
            click.echo(f"Re-exported user {user_uuid} to OS2sync")


@click.group(
    help="""
        Intended usage:

        1. List affected users:
        $ python -m os2sync_export.fix_fkorg_duplicated_positions find

        2. Remove affected users from FK ORG:
        $ python -m os2sync_export.fix_fkorg_duplicated_positions remove_from_fkorg

        3. Remove affected users from OS2sync (`success_users` table in MySQL):
        $ python -m os2sync_export.fix_fkorg_duplicated_positions remove_from_os2sync

        4. Re-export affected users from MO to OS2sync:
        $ python -m os2sync_export.fix_fkorg_duplicated_positions redo_export
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


@cli.command(help="Emit SQL to remove users from OS2sync `success_users` table")
@click.pass_context
def remove_from_os2sync(ctx):
    click.echo("SQL to remove users from OS2sync:")
    click.echo(_remove_from_os2sync(ctx.obj["user_uuids_to_fix"]))


@cli.command(help="Re-export affected users from MO to OS2sync")
@click.pass_context
def redo_export(ctx, dry_run):
    click.echo(f"Re-exporting {len(ctx.obj['user_uuids_to_fix'])} users to OS2sync ...")
    _redo_export(ctx.obj["settings"], ctx.obj["user_uuids_to_fix"], dry_run=False)


if __name__ == "__main__":
    cli(obj={})
