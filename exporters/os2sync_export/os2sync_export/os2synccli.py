import asyncio
from functools import partial
from typing import Dict
from typing import List
from uuid import UUID

import click
import httpx
from os2sync_export import lcdb_os2mo
from os2sync_export import os2mo
from os2sync_export import os2sync
from os2sync_export.cleanup_mo_uuids import remove_from_os2sync
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import Settings
from os2sync_export.config import setup_gql_client
from os2sync_export.os2mo import get_sts_orgunit
from os2sync_export.os2mo import get_sts_user


def update_single_user(uuid: UUID, settings: Settings, dry_run: bool) -> List[Dict]:
    if settings.os2sync_use_lc_db:
        engine = lcdb_os2mo.get_engine()
        session = lcdb_os2mo.get_session(engine)
        os2mo.get_sts_user_raw = partial(lcdb_os2mo.get_sts_user_raw, session)

    gql_client = setup_gql_client(settings)
    with gql_client as gql_session:
        sts_users = get_sts_user(str(uuid), gql_session=gql_session, settings=settings)

    if dry_run:
        return sts_users

    for sts_user in sts_users:
        if sts_user["Positions"]:
            os2sync.upsert_user(sts_user)
        else:
            os2sync.delete_user(str(uuid))
    return sts_users


async def update_single_orgunit(
    client: httpx.AsyncClient, uuid: UUID, settings: Settings, dry_run: bool
) -> Dict:

    org_unit = get_sts_orgunit(str(uuid), settings=settings)

    if dry_run:
        return org_unit

    await os2sync.upsert_orgunit(client, org_unit)

    return org_unit


@click.group()
def cli():
    pass


@cli.command()
@click.argument("uuid", type=click.UUID)
@click.option("--dry-run", is_flag=True)
def update_user(uuid: UUID, dry_run: bool):
    """Send os2sync payload for a single user"""
    settings = get_os2sync_settings()
    settings.start_logging_based_on_settings()

    click.echo(update_single_user(uuid, settings, dry_run))


@cli.command()
@click.argument("uuid", type=click.UUID)
@click.option("--dry-run", is_flag=True)
def update_org_unit(uuid: UUID, dry_run: bool):
    """Send os2sync payload for a single org_unit"""
    settings = get_os2sync_settings()

    async def client_setup(uuid):
        async with httpx.AsyncClient() as async_os2sync_client:
            return update_single_orgunit(
                async_os2sync_client,
                uuid,
                settings,
                dry_run,
            )

    click.echo(asyncio.run(client_setup(uuid)))


@cli.command()
@click.option("--dry-run", is_flag=True)
def cleanup_mo_uuids(dry_run: bool):
    """Remove objects with mo uuids from fk-org when their uuid is set in an it-system.
    Used for when an existing object has been given an fk-org uuid in an it-system

    """
    settings = get_os2sync_settings()
    gql_client = setup_gql_client(settings)
    with gql_client as gql_session:
        res = remove_from_os2sync(
            gql_session=gql_session, settings=settings, dry_run=dry_run
        )
    if res is None:
        click.echo("No it-system set in settings as os2sync_uuid_from_it_systems.")
        return

    org_units, employees = res
    click.echo(len(org_units))
    click.echo(len(employees))


if __name__ == "__main__":
    cli()
