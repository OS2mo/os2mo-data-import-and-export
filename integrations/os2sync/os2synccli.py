import click

from integrations.os2sync import os2sync
from integrations.os2sync.config import get_os2sync_settings
from integrations.os2sync.os2mo import get_sts_orgunit
from integrations.os2sync.os2mo import get_sts_user


@click.group()
def cli():
    pass


@cli.command()
@click.argument("uuid", type=click.UUID)
@click.option("--dry-run", is_flag=True)
def update_user(uuid, dry_run):
    """Send os2sync payload for a single user"""
    settings = get_os2sync_settings()

    sts_user = get_sts_user(str(uuid), settings=settings)

    if dry_run:
        click.echo(sts_user)
        return

    if sts_user["Positions"]:
        os2sync.upsert_user(sts_user)
    else:
        os2sync.delete_user(str(uuid))


@cli.command()
@click.argument("uuid", type=click.UUID)
@click.option("--dry-run", is_flag=True)
def update_org_unit(uuid, dry_run):
    """Send os2sync payload for a single org_unit"""
    settings = get_os2sync_settings()

    sts_org_unit = get_sts_orgunit(str(uuid), settings=settings)

    if dry_run:
        click.echo(sts_org_unit)
        return

    os2sync.upsert_orgunit(sts_org_unit)


if __name__ == "__main__":
    cli()
