from functools import partial
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

import click
from os2sync_export import lcdb_os2mo
from os2sync_export import os2mo
from os2sync_export import os2sync
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import Settings
from os2sync_export.config import setup_gql_client
from os2sync_export.os2mo import get_sts_orgunit
from os2sync_export.os2mo import get_sts_user


def update_single_user(
    uuid: UUID, settings: Settings, dry_run: bool
) -> List[Optional[Dict]]:
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
        if sts_user:
            os2sync.upsert_user(sts_user)

    return sts_users


def update_single_orgunit(
    uuid: UUID, settings: Settings, dry_run: bool
) -> Optional[Dict]:

    sts_org_unit = get_sts_orgunit(str(uuid), settings=settings)

    if dry_run:
        return sts_org_unit
    if sts_org_unit:
        os2sync.upsert_orgunit(sts_org_unit)
    return sts_org_unit


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
    click.echo(
        update_single_orgunit(
            uuid,
            settings,
            dry_run,
        )
    )


if __name__ == "__main__":
    cli()
