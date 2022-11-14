from typing import Dict
from uuid import UUID

import click
from os2sync_export import os2sync
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import Settings
from os2sync_export.os2mo import get_sts_orgunit
from os2sync_export.os2mo import get_sts_user


def update_single_user(uuid: UUID, settings: Settings, dry_run: bool) -> Dict:
    sts_user = get_sts_user(str(uuid), settings=settings)

    if dry_run:
        return sts_user

    if sts_user["Positions"]:
        os2sync.upsert_user(sts_user)
    else:
        os2sync.delete_user(str(uuid))
    return sts_user


def update_single_orgunit(uuid: UUID, settings: Settings, dry_run: bool) -> Dict:

    sts_org_unit = get_sts_orgunit(str(uuid), settings=settings)

    if dry_run:
        return sts_org_unit

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

    click.echo(update_single_user(uuid, settings, dry_run))


@cli.command()
@click.argument("uuid", type=click.UUID)
@click.option("--dry-run", is_flag=True)
def update_org_unit(uuid: UUID, dry_run: bool):
    """Send os2sync payload for a single org_unit"""
    settings = get_os2sync_settings()
    click.echo(
        update_org_unit(
            uuid,
            settings,
            dry_run,
        )
    )


if __name__ == "__main__":
    cli()
