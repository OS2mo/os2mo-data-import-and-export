import click

from sd_mox import SDMox
from util import async_to_sync, first_of_month

clickDate = click.DateTime(formats=["%Y-%m-%d"])


@click.group()
@click.option(
    "--from-date",
    type=clickDate,
    default=str(first_of_month()),
    help="TODO",
    show_default=True,
)
@click.option("--to-date", type=clickDate, help="TODO")
@click.option("--overrides", multiple=True)
@click.pass_context
def sd_mox_cli(ctx, from_date, to_date, overrides):
    """Tool to make changes in SD."""

    from_date = from_date.date()
    to_date = to_date.date() if to_date else None
    if to_date and from_date > to_date:
        raise click.ClickException("from_date must be smaller than to_date")

    overrides = dict(override.split("=") for override in overrides)

    sdmox = SDMox(from_date, to_date, overrides=overrides)

    ctx.ensure_object(dict)
    ctx.obj["sdmox"] = sdmox
    ctx.obj["from_date"] = from_date
    ctx.obj["to_date"] = to_date


@sd_mox_cli.command()
@click.pass_context
@click.option("--unit-uuid", type=click.UUID, required=True)
@click.option("--print-department", is_flag=True, default=False)
@click.option("--unit-name")
@async_to_sync
async def check_name(ctx, unit_uuid, print_department, unit_name):
    mox = ctx.obj["sdmox"]

    unit_uuid = str(unit_uuid)
    department, errors = await mox._check_department(
        unit_uuid=unit_uuid,
        unit_name=unit_name,
    )
    if print_department:
        import json

        print(json.dumps(department, indent=4))

    if errors:
        click.echo("Mismatches found for:")
        for error in errors:
            click.echo("* " + click.style(error, fg="red"))


@sd_mox_cli.command()
@click.pass_context
@click.option("--unit-uuid", type=click.UUID, required=True)
@click.option("--new-unit-name")
@click.option("--dry-run", is_flag=True, default=False)
@async_to_sync
async def set_name(ctx, unit_uuid, new_unit_name, dry_run):
    unit_uuid = str(unit_uuid)

    mox = ctx.obj["sdmox"]
    await mox.rename_unit(
        unit_uuid, new_unit_name, at=ctx.obj["from_date"], dry_run=dry_run
    )


if __name__ == "__main__":
    sd_mox_cli()
