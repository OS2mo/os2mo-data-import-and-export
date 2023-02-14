from datetime import date

import click

from sdlon.sdclient.client import SDClient
from sdlon.sdclient.requests import GetDepartmentRequest, GetEmploymentRequest


@click.group()
@click.option(
    "--username",
    "username",
    type=click.STRING,
    required=True,
    help="SD username"
)
@click.option(
    "--password",
    "password",
    type=click.STRING,
    required=True,
    help="SD password"
)
@click.option(
    "--institution-identifier",
    "institution_identifier",
    type=click.STRING,
    required=True,
    help="SD institution identifier"
)
@click.pass_context
def cli(ctx, username, password, institution_identifier):
    ctx.ensure_object(dict)

    ctx.obj["username"] = username
    ctx.obj["password"] = password
    ctx.obj["institution_identifier"] = institution_identifier


@cli.command()
@click.pass_context
def get_department(ctx):
    sd_client = SDClient(ctx.obj["username"], ctx.obj["password"])

    query_params = GetDepartmentRequest(
        InstitutionIdentifier=ctx.obj["institution_identifier"],
        ActivationDate=date(2023, 1, 1),
        DeactivationDate=date(2023, 12, 31),
    )

    r = sd_client.get_department(query_params)
    click.echo(r)


@cli.command()
@click.pass_context
def get_employment(ctx):
    sd_client = SDClient(ctx.obj["username"], ctx.obj["password"])

    query_params = GetEmploymentRequest(
        InstitutionIdentifier=ctx.obj["institution_identifier"],
        EffectiveDate=date(2023, 2, 7),
        EmploymentStatusIndicator = True,
    )

    r = sd_client.get_employment(query_params)
    click.echo(r)


if __name__ == "__main__":
    cli(obj=dict())
