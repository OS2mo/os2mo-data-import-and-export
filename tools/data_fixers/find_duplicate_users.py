from typing import Dict
from typing import List
from operator import itemgetter

import click
import requests
from more_itertools import bucket
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_settings
from ra_utils.apply import apply


def check_duplicate_cpr(mora_base: str) -> Dict[str, List[str]]:

    helper = MoraHelper(hostname=mora_base)
    users = helper.read_all_users()
    users = filter(lambda u: u.get("cpr_no"), users)
    cprs = dict(map(itemgetter("uuid", "cpr_no"), users))
    duplicates = [i for i, cnt in Counter(cprs.values()).items() if cnt > 1]
    duplicate_uuids = dict(filter(lambda x: x[1] in duplicates, cprs.items()))
    return duplicate_uuids


@click.command()
@click.option(
    "--delete",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Remove all user objects that has the same cpr-number as another user",
)
@click.option(
    "--keep-one",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Instead of removing all, remove all, but one per duplicate set",
)
@click.option(
    "--dry-run",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Write out all duplicates",
)
@click.option(
    "--verbose",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Write out all duplicates",
)
def cli(delete: bool, keep_one: bool, dry_run: bool, verbose: bool):
    """Find users in MO that have the same CPR number.

    Prints the number of cpr-numbers that are used by more than one user and the list of uuids for the users sharing a cpr-number.
    Add the `--delete` flag to remove all users that share the same cpr-number of another MO user.
    """
    settings = load_settings()
    mox_base = settings.get("mox.base")
    mora_base = settings.get("mora.base")
    duplicate_dict = check_duplicate_cpr(mora_base)

    if verbose:
        for cpr_no, uuids in duplicate_dict.items():
            click.echo(f"CPR: {cpr_no}, with UUIDS:")
            for uuid in uuids:
                click.echo(f"\t{uuid}")

    if delete:
        for cpr_no, uuids in duplicate_dict.items():
            # If we are keeping one, forward iterator once
            uuids = iter(uuids)
            if keep_one:
                next(uuids)
            for uuid in uuids:
                if dry_run:
                    click.echo(f"Would have deleted {uuid}")
                else:
                    if verbose:
                        click.echo(f"Deleting {uuid}")
                    r = requests.delete(f"{mox_base}/organisation/bruger/{uuid}")
                    r.raise_for_status()
    else:
        if keep_one:
            raise click.ClickException("Cannot pass 'keep_one' without 'delete'")
        click.echo(
            f"There are {len(duplicate_dict)} CPR-number(s) assigned to more than one user"
        )


if __name__ == "__main__":
    cli()
