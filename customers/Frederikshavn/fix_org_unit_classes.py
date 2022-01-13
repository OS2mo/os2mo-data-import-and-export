from itertools import groupby

import click
import requests
from more_itertools import only
from more_itertools import partition
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_setting
from tqdm import tqdm

from tools.data_fixers.class_tools import delete_class
from tools.data_fixers.class_tools import move_class_helper


def split(group):
    name, classes = group
    classes = list(classes)
    no_scope, scope = partition(lambda x: x.get("owner") and x.get("scope"), classes)
    return only(no_scope), only(scope)


def is_duplicate(classes):
    no_scope, scope = classes
    return no_scope and scope




@click.command()
@click.option(
    "--mox-base",
    help="URL for MOX",
    type=click.STRING,
    default=load_setting("mox.base", "http://localhost:8080/"),
)
@click.option(
    "--mora-base",
    help="URL for MO",
    type=click.STRING,
    default=load_setting("mora.base", "http://localhost:5000/"),
)
@click.option("--dry-run", is_flag=True)
def cli(mox_base: str, mora_base: str, dry_run: bool):

    helper = MoraHelper(hostname=mora_base)

    org_unit_types, _ = helper.read_classes_in_facet("org_unit_type")
    org_unit_types=sorted(org_unit_types, key=lambda x: x['name'])
    groups = groupby(org_unit_types, key=lambda x: x["name"])

    split_classes = map(split, groups)
    split_classes = filter(is_duplicate, split_classes)
    if dry_run:
        click.echo(
            f"Dry-run: Found {len(list(split_classes))} duplicated classes to fix."
        )
        return

    session = requests.session()
    
    for no_scope, scope in tqdm(split_classes, desc="Moving relations to one class"):
        old_uuid = no_scope["uuid"]
        move_class_helper(old_uuid=old_uuid, new_uuid=scope["uuid"], copy=False, mox_base=mox_base, relation_type='organisation/organisationenhed')
    
        delete_class(session=session, base=mox_base, uuid=old_uuid)


if __name__ == "__main__":
    cli()
