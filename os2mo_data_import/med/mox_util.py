import json
import sys
from typing import Tuple

import click

from mox_helper import create_mox_helper
from payloads import lora_facet, lora_klasse
from utils import async_to_sync, dict_map


@click.group()
@click.option(
    "--settings-file",
    default="settings.json",
    type=click.Path(resolve_path=True),
    help="Path to the settings.json file",
)
@click.pass_context
def settings_loader(ctx, settings_file: str):
    """MOX Util settings loader.

    This command-level only serves to load settings files for the lower command
    levels, so please refer to the `cli` level for the actual functionality.
    
    Please run: python mox_util.py cli
    """
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
        # i.e. if a file argument is passed, the file should exist.
        if not settings_file_from_default:
            raise exp

    def fix_key(key: str, value: str) -> Tuple[str, str]:
        """Fixup keys from settings.json to click format."""
        # NOTE: This is most likely not complete at all
        return key.replace(".", "_"), value

    # Apply fix key to each item tuple in settings
    settings = dict_map(fix_key, settings)

    # Loaded settings are default for cli, thus overriding the coded defaults,
    # but them themselves being overridden by commandline arguments.
    ctx.default_map = {"cli": settings}


@settings_loader.group()
@click.option(
    "--mox-base",
    default="http://localhost:8080",
    show_default=True,
    help="Address of the MOX host",
)
@click.pass_context
def cli(ctx, mox_base: str):
    """MOX Util CLI."""
    ctx.ensure_object(dict)
    # I expect several other keys might be nice to load from settings, or
    # atleast that this multilevel group scheme could be used for exporters.
    ctx.obj["mox.base"] = mox_base


def print_created(uuid: str, created: bool) -> None:
    """Output uuid followed by created or exists.

    The color of the output follows Ansibles changed / unchanged coor scheme.
    """
    output_map = {
        False: {"message": "exists", "color": "green",},
        True: {"message": "created", "color": "yellow",},
    }
    output = output_map[created]
    click.secho(uuid + " " + output["message"], fg=output["color"])


@cli.command()
@click.pass_context
@click.option(
    "--bvn",
    "--brugervendt-nøgle",
    required=True,
    help="User key to set on the class.",
)
@click.option("--title", required=True, help="Title to set on the class.")
@click.option(
    "--facet-bvn", required=True, help="User key of the facet to bind the class to."
)
@click.option("--description", help="Description to set on the class.")
@click.option(
    "--org-uuid", "--organisation", help="Organisation to bind the facet to."
)
@click.option("--parent-bvn", help="User key of another class to put this under.")
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry run and print the generated object.",
)
@async_to_sync
async def ensure_class_exists(
    ctx,
    bvn: str,
    title: str,
    facet_bvn: str,
    description: str,
    org_uuid: str,
    parent_bvn: str,
    dry_run: bool,
):
    """Ensure the generated class exists in MOX."""
    mox_helper = await create_mox_helper(ctx.obj["mox.base"])

    # Fetch default organisation if any, assuming none is set
    org_uuid = org_uuid or await mox_helper.read_element_organisation_organisation(
        bvn="%"
    )

    # Fetch uuids from bvns
    facet_uuid = await mox_helper.read_element_klassifikation_facet(bvn=facet_bvn)
    parent_uuid = None
    if parent_bvn:
        parent_uuid = await mox_helper.read_element_klassifikation_klasse(
            bvn=parent_bvn
        )

    # Generate dict
    klasse = lora_klasse(
        bvn,
        title,
        facet_uuid,
        org_uuid,
        description=description,
        overklasse=parent_uuid,
    )

    # Print for dry run
    if dry_run:
        mox_helper.validate_klassifikation_klasse(klasse)
        message = json.dumps(klasse, indent=4, sort_keys=True)
        click.secho(message, fg="green")
        return

    # POST for non-dry
    mox_helper = await create_mox_helper(ctx.obj["mox.base"])
    uuid, created = await mox_helper.get_or_create_klassifikation_klasse(klasse)
    print_created(uuid, created)


@cli.command()
@click.pass_context
@click.option(
    "--bvn",
    "--brugervendt-nøgle",
    required=True,
    help="User key to set on the facet.",
)
@click.option("--description", help="Description to set on the facet.")
@click.option(
    "--org-uuid", "--organisation", help="Organisation to bind the facet to."
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry run and print the generated object.",
)
@async_to_sync
async def ensure_facet_exists(
    ctx, bvn: str, description: str, org_uuid: str, dry_run: bool
):
    """Ensure the generated facet exists in MOX."""
    mox_helper = await create_mox_helper(ctx.obj["mox.base"])

    # Fetch default organisation if any, assuming none is set
    org_uuid = org_uuid or await mox_helper.read_element_organisation_organisation(
        bvn="%"
    )

    # Generate dict
    facet = lora_facet(bvn, org_uuid, description)

    # Print for dry run
    if dry_run:
        mox_helper.validate_klassifikation_facet(facet)
        print(json.dumps(facet, indent=4, sort_keys=True))
        return

    # POST for non-dry
    uuid, created = await mox_helper.get_or_create_klassifikation_facet(facet)
    print_created(uuid, created)


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
    exit_print_on_success: str,
    exit_print_on_error: str,
    exit_code_on_success: int,
    exit_code_on_error: int,
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
    async def is_up() -> bool:
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
