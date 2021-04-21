import json
import urllib.parse
from collections import Counter
from operator import itemgetter
from typing import List, Tuple

import click
import jmespath
import requests
from more_itertools import only
from tqdm import tqdm

from exporters.utils.load_settings import load_settings

jms_bvn_list = jmespath.compile(
    "[*].registreringer[0].attributter.klasseegenskaber[0].brugervendtnoegle"
)
jms_bvn_one = jmespath.compile(
    "registreringer[0].attributter.klasseegenskaber[0].brugervendtnoegle"
)


def check_relations(session, base: str, uuid: str) -> List[dict]:
    """Find all objects related to the class with the given uuid.

    Returns a list of objects, or an empty list if no objects related to the given uuid are found.
    """
    r = session.get(
        base + f"/organisation/organisationfunktion?vilkaarligrel={uuid}&list=true"
    )
    r.raise_for_status()
    res = r.json()["results"]
    return only(res, default=[])


def read_duplicate_class(session, base: str, bvn: str) -> List[Tuple[str, str]]:
    """Read details of classes with the given bvn.

    Returns a list of tuples with uuids and bvns of the found classes.
    """
    bvn = urllib.parse.quote(bvn)
    r = session.get(base + f"/klassifikation/klasse?brugervendtnoegle={bvn}&list=true")
    r.raise_for_status()
    res = r.json()["results"][0]
    uuids = jmespath.search("[*].id", res)
    bvns = jms_bvn_list.search(res)
    return list(zip(uuids, bvns))


def delete_class(session, base: str, uuid: str) -> None:
    """Delete the class with the given uuid."""
    r = session.delete(base + f"/klassifikation/klasse/{uuid}")
    r.raise_for_status()


def switch_class(session, base: str, payload: str, new_uuid: str) -> None:
    """Switch an objects related class.

    Given an object payload and an uuid this function wil switch the class that an object is related to.
    """
    old_uuid = payload["id"]
    payload = payload["registreringer"][0]
    payload = {
        item: payload.get(item) for item in ("attributter", "relationer", "tilstande")
    }
    payload["relationer"]["organisatoriskfunktionstype"][0]["uuid"] = new_uuid
    r = session.patch(
        base + f"/organisation/organisationfunktion/{old_uuid}/", json=payload
    )
    r.raise_for_status()


def find_duplicates_classes(session, mox_base:str) -> List[str]:
    """Find classes that are duplicates and return them."""
    r = session.get(mox_base + "/klassifikation/klasse?list=true")
    all_classes = r.json()["results"][0]
    all_ids = map(itemgetter("id"), all_classes)
    all_classes = list(map(lambda c: jms_bvn_one.search(c).lower(), all_classes))
    class_map = dict(zip(all_classes, all_ids))
    duplicate_list = [i for i, cnt in Counter(all_classes).items() if cnt > 1]
    return duplicate_list


@click.command()
@click.option(
    "--delete",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Remove any class that has duplicates",
)
def cli(delete):
    """Tool to help remove classes from MO that are duplicates.

    This tool is written to help clean up engagement_types that had the same name, but with different casing.
    If no argument is given it will print the amount of duplicated classses.
    If the `--delete` flag is supplied you will be prompted to choose a class to keep for each duplicate.
    Objects related to the other class will be transferred to the selected class and the other class deleted.
    """

    settings = load_settings()
    mox_base = settings.get("mox.base", "http://localhost:8080/")
    session = requests.Session()

    duplicate_list = find_duplicates_classes(session=session, mox_base=mox_base)

    if not delete:
        click.echo(f"There are {len(duplicate_list)} duplicate class(es).")
        return

    for dup in tqdm(duplicate_list, desc="Deleting duplicate classes"):

        dup_class = read_duplicate_class(session, mox_base, dup)
        bvn_set = set(map(itemgetter(1), dup_class))
        # Check if all found bvns are exactly the same. Only prompt for a choice if they are not.
        keep = 1
        if len(bvn_set) != 1:
            click.echo("These are the choices:")
            # Generate a prompt to display
            msg = "\n".join(f"  {i}: {bvn}" for i, bvn in enumerate(bvn_set, start=1))
            click.echo(msg)
            keep = click.prompt("Choose the one to keep", type=int, default=1)
        kept_uuid, _ = dup_class[keep - 1]
        for i, obj in enumerate(dup_class, start=1):
            if i == keep:
                continue
            uuid, _ = obj
            rel = check_relations(session, mox_base, uuid)
            for payload in tqdm(rel, desc="Changing class for objects"):
                switch_class(session, mox_base, payload, kept_uuid)
            delete_class(session, mox_base, uuid)


if __name__ == "__main__":
    cli()
