from collections import Counter
from operator import itemgetter

import click
import requests
from os2mo_helpers.mora_helpers import MoraHelper

from exporters.utils.load_settings import load_settings


def check_duplicate_cpr() -> dict:
    settings = load_settings()
    mora_base = settings.get("mora.base", "http://localhost:5000/")
    helper = MoraHelper(hostname=mora_base)
    users = helper.read_all_users()
    cprs = dict(map(itemgetter("uuid", "cpr_no"), users))
    ldupl = [i for i, cnt in Counter(cprs.values()).items() if cnt > 1]
    duplicate_uuids = dict(filter(lambda x: x[1] in ldupl, cprs.items()))

    print(
        f"There are {len(ldupl)} CPR-number(s) assigned to more than one user",
        list(duplicate_uuids.keys()),
    )

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
def cli(delete):
    """Tool to help find or delete users in MO have the same CPR number.

    This tool is written to help clean up users with same cpr.
    """
    settings = load_settings()
    mox_base = settings.get("mox.base")
    uuids = check_duplicate_cpr()
    if delete:
        for uuid in uuids:
            r = requests.delete(f"{mox_base}/organisation/bruger/{uuid}")
            r.raise_for_status()


if __name__ == "__main__":
    cli()
