import click
from uuid import UUID
from functools import lru_cache

from ra_utils.apply import apply
from ra_utils.load_settings import load_setting
from os2mo_helpers.mora_helpers import MoraHelper

IT_SYSTEM_NAME = "Active Directory GUID"


@lru_cache
def _ensure_itsystem_exists() -> UUID:
    return UUID("db519bfd-0fdd-4e5d-9337-518d1dbdbfc9")


def _write_ad_guid_to_mo_user(
    mora_helper: MoraHelper,
    ad_guid: UUID,
    mo_uuid: UUID,
):
    """Write user AD username to the AD it system"""
    it_system_uuid: UUID = _ensure_itsystem_exists()

    payload = {
        "type": "it",
        "user_key": str(ad_guid),
        "itsystem": {"uuid": str(it_system_uuid)},
        "person": {"uuid": str(mo_uuid)},
        "validity": {"from": "1930-01-01", "to": None},
    }
    response = mora_helper._mo_post("details/create", payload)
    assert response.status_code == 201


def sync_user(mora_helper: MoraHelper, uuid: UUID, cpr: str):
    print(uuid, cpr)


@click.group()
@click.option(
    "--mora-base",
    default=load_setting("mora.base", "http://localhost:5000"),
    help="URL for OS2mo.",
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dump payload, rather than writing to rollekataloget.",
)
@click.pass_context
def cli(ctx, mora_base: str, dry_run: bool):
    ctx.ensure_object(dict)
    ctx.obj = dict()
    ctx.obj["dry_run"] = dry_run
    ctx.obj["mora_base"] = mora_base
    ctx.obj["mora_helper"] = MoraHelper(hostname=mora_base, use_cache=False)


@cli.command()
@click.option(
    "--uuid", type=click.UUID, help="UUID of the user to check.", required=True
)
@click.pass_context
def sync_uuid(ctx, uuid: UUID):
    mora_helper = ctx.obj["mora_helper"]
    employee = mora_helper.read_user(user_uuid=str(uuid))
    sync_user(UUID(employee["uuid"]), employee["cpr_no"])


@cli.command()
@click.option(
    "--cpr", help="CPR of the user to check.", required=True
)
@click.pass_context
def sync_cpr(ctx, cpr: str):
    mora_helper = ctx.obj["mora_helper"]
    employee = mora_helper.read_user(user_cpr=cpr)
    sync_user(UUID(employee["uuid"]), employee["cpr_no"])


@cli.command()
@click.pass_context
def sync_all(ctx):
    mora_helper = ctx.obj["mora_helper"]
    mora_helper.host = ctx.obj["mora_base"]
    it_employees = mora_helper._mo_lookup(None, url="/api/v1/it")
    it_employees = filter(
        lambda it_employee: UUID(it_employee["itsystem"]["uuid"]) == _ensure_itsystem_exists(),
        it_employees
    )
    it_employees = set(map(
        lambda it_employee: UUID(it_employee["person"]["uuid"]),
        it_employees
    ))

    employees = mora_helper._mo_lookup(None, url="/api/v1/employee")
    employees = filter(
        lambda employee: UUID(employee["uuid"]) not in it_employees,
        employees
    )
    for employee in employees:
        sync_user(UUID(employee["uuid"]), employee["cpr_no"])


if __name__ == "__main__":
    cli()
