import json
import urllib.parse
from collections import Counter
from operator import itemgetter
from typing import Dict, List, Set, Tuple
from uuid import UUID

import click
import jmespath
import requests
from more_itertools import only, unzip
from tqdm import tqdm

from exporters.utils.load_settings import load_settings

jms_bvn = jmespath.compile(
    "registreringer[0].attributter.klasseegenskaber[0].brugervendtnoegle"
)
jms_title = jmespath.compile("registreringer[0].attributter.klasseegenskaber[0].titel")
jms_facet = jmespath.compile("registreringer[0].relationer.facet[0].uuid")


def check_relations(session, base: str, uuid: UUID) -> List[dict]:
    """Find all objects related to the class with the given uuid.

    Returns a list of objects, or an empty list if no objects related to the given uuid are found.
    """
    r = session.get(
        base
        + f"/organisation/organisationfunktion?vilkaarligrel={str(uuid)}&list=true&virkningfra=-infinity"
    )
    r.raise_for_status()
    res = r.json()["results"]
    return only(res, default=[])


def delete_class(session, base: str, uuid: UUID) -> None:
    """Delete the class with the given uuid."""
    r = session.delete(base + f"/klassifikation/klasse/{str(uuid)}")
    r.raise_for_status()


def switch_class(
    session, base: str, payload: str, new_uuid: UUID, uuid_set: Set[str]
) -> None:
    """Switch an objects related class.

    Given an object payload and an uuid this function wil switch the class that an object is related to.
    Only switches class if it is in the set uuid_set.
    """
    old_uuid = UUID(payload["id"])
    payload = payload["registreringer"][0]
    # Drop data we don't need to post
    payload = {
        item: payload.get(item) for item in ("attributter", "relationer", "tilstande")
    }

    # Change all uuids from uuid_set to new_uuid.
    p_string = json.dumps(payload)
    for old_uuid in uuid_set:
        p_string = p_string.replace(str(old_uuid), str(new_uuid))
    payload = json.loads(p_string)

    r = session.put(
        base + f"/organisation/organisationfunktion/{str(old_uuid)}", json=payload
    )
    r.raise_for_status()


def find_duplicates_classes(session, mox_base: str) -> List[List[Tuple[UUID, str]]]:
    """Find classes within a facet that are duplicates.

    Returns a list of lists containing uuids and titles of classes that are duplicates.
    """
    r = session.get(mox_base + "/klassifikation/klasse?list=true")
    all_classes = r.json()["results"][0]
    # gather relevant data: uuid of class, bvn, titles and facet uuid.
    all_ids = list(map(itemgetter("id"), all_classes))
    all_class_bvns = list(map(lambda c: jms_bvn.search(c), all_classes))
    all_class_titles = list(map(lambda c: jms_title.search(c), all_classes))
    all_facets = list(map(lambda c: jms_facet.search(c), all_classes))

    # find duplicates of (lowercase bvn, facet uuid)
    all_class_bvns_lower = [x.lower() for x in all_class_bvns]
    bvn_facets_lower = list(zip(all_class_bvns_lower, all_facets))
    dup_bvn_facets = set(x for x in bvn_facets_lower if bvn_facets_lower.count(x) > 1)

    # We need to return class uuids and original case title for each duplicate.
    title_map = dict(zip(all_ids, all_class_titles))
    bvn_map_lower = dict(zip(all_ids, bvn_facets_lower))

    duplicate_bvn_facet = [
        [(uuid, title_map[uuid]) for uuid, val in bvn_map_lower.items() if val == dup]
        for dup in dup_bvn_facets
    ]
    return duplicate_bvn_facet


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
    In case there are no differences to
    Objects related to the other class will be transferred to the selected class and the other class deleted.
    """

    settings = load_settings()
    mox_base = settings.get("mox.base", "http://localhost:8080/")
    session = requests.Session()

    duplicate_bvn_facet = find_duplicates_classes(session=session, mox_base=mox_base)

    if not delete:
        click.echo(f"There are {len(duplicate_bvn_facet)} duplicate class(es).")
        return

    for dup_class in tqdm(duplicate_bvn_facet, desc="Deleting duplicate classes"):
        uuids, titles = unzip(dup_class)
        uuid_set = set(uuids)
        title_set = set(titles)

        # Check if all found titles are exactly the same. Only prompt for a choice if they are not.
        keep = 1
        if len(title_set) != 1:
            click.echo("These are the choices:")
            # Generate a prompt to display
            msg = "\n".join(f"  {i}: {x[1]}" for i, x in enumerate(dup_class, start=1))
            click.echo(msg)
            keep = click.prompt("Choose the one to keep", type=int, default=1)
        kept_uuid, _ = dup_class[keep - 1]
        for i, obj in enumerate(dup_class, start=1):
            if i == keep:
                continue
            uuid, _ = obj
            rel = check_relations(session, mox_base, uuid)
            for payload in tqdm(rel, desc="Changing class for objects"):
                switch_class(session, mox_base, payload, kept_uuid, uuid_set)
            delete_class(session, mox_base, uuid)


if __name__ == "__main__":
    cli()
