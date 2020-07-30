import json
import sys

import click

from mox_helper import create_mox_helper
from payloads import lora_facet, lora_klasse
from utils import async_to_sync, dict_map


@click.group(invoke_without_command=True)
@click.option(
    "--settings-file", default="settings.json", type=click.Path(resolve_path=True)
)
@click.pass_context
def settings_loader(ctx, settings_file):
    #    # Not available until click 8
    #    settings_file_from_default = (
    #        ctx.get_parameter_source("settings-file") == click.ParameterSource.DEFAULT
    #    )
    settings_file_from_default = True
    settings = {}
    try:
        with click.open_file(settings_file, "rb") as f:
            settings = json.load(f)
    except FileNotFoundError as exp:
        # Only default file is allowed not to exist
        if not settings_file_from_default:
            raise exp

    def fix_key(key, value):
        return key.replace(".", "_"), value

    settings = dict_map(fix_key, settings)
    # Loaded settings are default for cli
    ctx.default_map = {"cli": settings}


@settings_loader.group()
@click.option("--mox-base", default="http://localhost:8080", show_default=True)
@click.pass_context
def cli(ctx, mox_base):
    ctx.ensure_object(dict)
    ctx.obj["mox.base"] = mox_base


@cli.command()
@click.pass_context
@click.option(
    "--bvn", "--brugervendt-nøgle", required=True, help="The person to greet."
)
@click.option("--description", help="The person to greet.")
@click.option("--title", required=True, help="The person to greet.")
@click.option("--facet-bvn", required=True, help="The person to greet.")
@click.option("--org-uuid", "--organisation", help="The person to greet.")
@click.option("--parent-bvn", help="The person to greet.")
@async_to_sync
async def ensure_class_exists(
    ctx, bvn, description, title, facet_bvn, org_uuid, parent_bvn
):
    mox_helper = await create_mox_helper(ctx.obj["mox.base"])
    if org_uuid is None:
        org_uuid = await mox_helper.read_element_organisation_organisation(bvn="%")
    facet_uuid = await mox_helper.read_element_klassifikation_facet(bvn=facet_bvn)
    parent_uuid = None
    if parent_bvn:
        parent_uuid = await mox_helper.read_element_klassifikation_klasse(
            bvn=parent_bvn
        )
    klasse = lora_klasse(
        bvn,
        title,
        facet_uuid,
        org_uuid,
        description=description,
        overklasse=parent_uuid,
    )
    uuid, created = await mox_helper.get_or_create_klassifikation_klasse(klasse)
    print(uuid, "created" if created else "exists")


@cli.command()
@click.pass_context
@click.option(
    "--bvn", "--brugervendt-nøgle", required=True, help="The person to greet."
)
@click.option("--description", help="The person to greet.")
@click.option("--org-uuid", "--organisation", help="The person to greet.")
@async_to_sync
async def ensure_facet_exists(ctx, bvn, description, org_uuid):
    mox_helper = await create_mox_helper(ctx.obj["mox.base"])
    if org_uuid is None:
        org_uuid = await mox_helper.read_element_organisation_organisation(bvn="%")
    facet = lora_facet(bvn, org_uuid, description)
    uuid, created = await mox_helper.get_or_create_klassifikation_facet(facet)
    print(uuid, "created" if created else "exists")


@cli.command()
@click.option(
    "--exit-print-on-success",
    show_default=True,
    type=click.STRING,
    help="String to print if able to connect",
)
@click.option(
    "--exit-print-on-error",
    show_default=True,
    default="Unable to connect",
    type=click.STRING,
    help="String to print if unable to connect",
)
@click.option(
    "--exit-code-on-success",
    show_default=True,
    default=0,
    type=click.IntRange(0, 255),
    help="Exit code if able to connect",
)
@click.option(
    "--exit-code-on-error",
    show_default=True,
    default=1,
    type=click.IntRange(0, 255),
    help="Exit code if unable to connect",
)
@click.pass_context
def check_connection(
    ctx,
    exit_print_on_success,
    exit_print_on_error,
    exit_code_on_success,
    exit_code_on_error,
):
    """Check whether a connection can be established to mox."""
    output_map = {
        False: {
            "exit_code": exit_code_on_error,
            "message": exit_print_on_error,
            "color": "red",
        },
        True: {
            "exit_code": exit_code_on_success,
            "message": exit_print_on_success,
            "color": "green",
        },
    }

    @async_to_sync
    async def is_up():
        mox_helper = await create_mox_helper(
            ctx.obj["mox.base"], generate_methods=False
        )
        return await mox_helper.check_connection()

    output = output_map[is_up()]
    if output["message"]:
        click.secho(output["message"], fg=output["color"])
    sys.exit(output["exit_code"])


@cli.command()
@click.pass_context
def noop(ctx):
    """Do nothing."""
    pass


if __name__ == "__main__":
    settings_loader(auto_envvar_prefix="MOXUTIL")
