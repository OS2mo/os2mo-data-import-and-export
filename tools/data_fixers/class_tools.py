import json
import urllib.parse
from collections import defaultdict
from operator import itemgetter
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple
from uuid import UUID
from uuid import uuid4

import click
import jmespath
import requests
from more_itertools import one
from more_itertools import only
from more_itertools import unzip
from ra_utils.load_settings import load_settings
from ra_utils.transpose_dict import transpose_dict
from tqdm import tqdm

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
    session,
    base: str,
    payload: str,
    new_uuid: UUID,
    uuid_set: Set[str],
    copy: bool = False,
) -> None:
    """Switch an objects related class.

    Given an object payload and an uuid this function wil switch the class that an object is related to.
    Only switches class if it is in the set uuid_set.
    """
    object_uuid = UUID(payload["id"])
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

    if copy:
        object_uuid = uuid4()

    r = session.put(
        base + f"/organisation/organisationfunktion/{str(object_uuid)}", json=payload
    )
    r.raise_for_status()


def read_classes(session, mox_base: str) -> List[Dict]:
    """Read all classes from MO"""
    r = session.get(mox_base + "/klassifikation/klasse?list=true")
    r.raise_for_status()
    return one(r.json()["results"])


def get_relevant_info(all_classes: List[Dict]) -> Tuple[List, List, List, List]:
    """From the full MO response find uuids, bvns and titles of the classes and the uuid of the facet the class exists in.

    >>> all_classes = [{'id': '13158594-13f8-4d16-85da-b8d7f00351d2', 'registreringer': [{'fratidspunkt': {'tidsstempeldatotid': '2021-04-28T17:46:41.27874+00:00', 'graenseindikator': True}, 'tiltidspunkt': {'tidsstempeldatotid': 'infinity'}, 'livscykluskode': 'Importeret', 'brugerref': '42c432e8-9c4a-11e6-9f62-873cf34a735f', 'attributter': {'klasseegenskaber': [{'brugervendtnoegle': 'test_bvn', 'omfang': 'TEXT', 'titel': 'Test Title', 'virkning': {'from': '1930-01-01 12:02:32+00', 'to': 'infinity', 'from_included': True, 'to_included': False}}]}, 'tilstande': {'klassepubliceret': [{'virkning': {'from': '1930-01-01 12:02:32+00', 'to': 'infinity', 'from_included': True, 'to_included': False}, 'publiceret': 'Publiceret'}]}, 'relationer': {'ansvarlig': [{'virkning': {'from': '1930-01-01 12:02:32+00', 'to': 'infinity', 'from_included': True, 'to_included': False}, 'uuid': '65b7feb6-0126-a033-550e-e52836610e1a', 'objekttype': 'organisation'}], 'facet': [{'virkning': {'from': '1930-01-01 12:02:32+00', 'to': 'infinity', 'from_included': True, 'to_included': False}, 'uuid': '2dd34236-c9ec-4059-9252-298f161e9b56'}]}}]}]
    >>> get_relevant_info(all_classes)
    (['13158594-13f8-4d16-85da-b8d7f00351d2'], ['test_bvn'], ['Test Title'], ['2dd34236-c9ec-4059-9252-298f161e9b56'])
    """
    class_uuids = list(map(itemgetter("id"), all_classes))
    class_bvns = list(map(lambda c: jms_bvn.search(c), all_classes))
    class_titles = list(map(lambda c: jms_title.search(c), all_classes))
    facet_uuids = list(map(lambda c: jms_facet.search(c), all_classes))
    return class_uuids, class_bvns, class_titles, facet_uuids


def filter_duplicates(
    class_uuids, class_bvns, class_titles, facet_uuids
) -> List[List[Tuple[UUID, str]]]:
    """Transforms data from classes to a list of duplicate classes in facets.

    Example 1) there are two classes called "test", but they are in different facets:
    >>> info = (["class_uuid1", "class_uuid2"],["test", "Test"],["TEST", "Test"],["facet_uuid1", "facet_uuid2"])
    >>> filter_duplicates(*info)
    {}

    Example 2) there are two classes called "test" in the same facet:
    >>> info = ["class_uuid1", "class_uuid2"],["test", "Test"],["TEST", "Test"],["facet_uuid1", "facet_uuid1"]
    >>> filter_duplicates(*info)
    {('test', 'facet_uuid1'): [('class_uuid1', 'TEST'), ('class_uuid2', 'Test')]}
    """
    # find duplicates of (lowercase bvn, facet uuid)
    class_bvns_lower = [x.lower() for x in class_bvns]
    bvn_facets_lower = list(zip(class_bvns_lower, facet_uuids))
    dup_bvn_facets = set(x for x in bvn_facets_lower if bvn_facets_lower.count(x) > 1)
    uuid_title_map = tuple(zip(class_uuids, class_titles))

    bvn_map_lower = dict(zip(uuid_title_map, bvn_facets_lower))
    # Find alle the duplicates
    duplicate_bvn_facet = filter(
        lambda x: x[1] in dup_bvn_facets, bvn_map_lower.items()
    )
    duplicate_bvn_facet = dict(duplicate_bvn_facet)

    # Transpose the dict to be able to iterate over duplicates
    transposed = transpose_dict(duplicate_bvn_facet)

    return transposed


def find_duplicates_classes(session, mox_base: str) -> List[List[Tuple[UUID, str]]]:
    """Find classes within a facet that are duplicates.

    Returns a list of lists containing uuids and titles of classes that are duplicates.
    """
    all_classes = read_classes(session, mox_base)
    info = get_relevant_info(all_classes)
    return filter_duplicates(*info)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--delete",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Remove any class that has duplicates",
)
@click.option(
    "--mox-base",
    help="URL for MOX",
    type=click.STRING,
    default=lambda: load_settings().get("mox.base", "http://localhost:8080/"),
)
def remove_dup_classes(delete: bool, mox_base: click.STRING):
    """Tool to help remove classes from MO that are duplicates.

    This tool is written to help clean up engagement_types that had the same name, but with different casing.
    If no argument is given it will print the amount of duplicated classses.
    If the `--delete` flag is supplied you will be prompted to choose a class to keep for each duplicate.
    In case there are no differences to
    Objects related to the other class will be transferred to the selected class and the other class deleted.
    """

    session = requests.Session()

    duplicate_bvn_facet = find_duplicates_classes(session=session, mox_base=mox_base)

    if not delete:
        click.echo(f"There are {len(duplicate_bvn_facet)} duplicate class(es).")
        return

    for dup_class in tqdm(
        duplicate_bvn_facet.values(), desc="Deleting duplicate classes"
    ):
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


@cli.command()
@click.option(
    "--old-uuid",
    required=True,
    type=click.UUID,
    help="UUID of old class",
)
@click.option(
    "--new-uuid",
    required=True,
    type=click.UUID,
    help="UUID of new class",
)
@click.option(
    "--copy",
    is_flag=True,
    help="Copy to a new object instead of switching class",
)
@click.option(
    "--mox-base",
    help="URL for MOX",
    type=click.STRING,
    default=lambda: load_settings().get("mox.base", "http://localhost:8080/"),
)
def move_class(old_uuid: click.UUID, new_uuid: click.UUID, copy: bool, mox_base: str):
    """Switches class, or copies to a new class for all objects using this class given two UUIDs.
    if --copy is supplied a new UUID will be generated for each object so that no objects are moved, only copied.
    """
    session = requests.Session()
    rel = check_relations(session, mox_base, old_uuid)
    for payload in tqdm(rel, desc="Changing class for objects"):
        switch_class(session, mox_base, payload, new_uuid, {old_uuid}, copy=copy)


if __name__ == "__main__":
    cli()
