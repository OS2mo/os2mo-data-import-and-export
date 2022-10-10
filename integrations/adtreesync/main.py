import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import groupby
from operator import itemgetter
from typing import Callable
from typing import Dict
from typing import IO
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import TypeVar
from uuid import UUID

import click
from asciitree import LeftAligned
from os2mo_helpers.settings import get_settings as get_mora_settings
from ra_utils.async_to_sync import async_to_sync
from raclients.modelclient.mo import ModelClient
from ramodels.mo import OrganisationUnit

from .config import get_ldap_settings
from .ldap import configure_ad_connection

CallableReturnType = TypeVar("CallableReturnType")
OrgTree = Dict[UUID, Dict]


@dataclass(eq=True, frozen=True)
class ParsedDN:
    dn: str
    parsed_dn: Tuple[str]


ADTree = Dict[UUID, ParsedDN]

MOOrganisationUnitMap = Dict[UUID, OrganisationUnit]


def parse_distinguished_name(dn: str) -> ParsedDN:
    dn_copy = dn[:]
    # Convert "\\," to "|"
    dn_copy = dn_copy.replace("\\,", "|")
    # Split on "," and replace "|" with ","
    split = [s.replace("|", ",") for s in dn_copy.split(",")]
    # ["OU=Foo", "DC="Bar"] -> [("OU", "Foo"), ("DC", "Bar")]
    categorized = map(lambda s: s.split("="), split)
    # [("OU", "Foo"), ("DC", "Bar")] -> ["Foo"]
    relevant = [value for (category, value) in categorized if category == "OU"]
    return ParsedDN(dn, tuple(reversed(relevant)))  # type: ignore


def load_ad_tree(settings, ad_connection) -> ADTree:
    ad_connection.search(
        search_base=settings["search_base"],
        search_filter="(objectclass=organizationalUnit)",
        # Search in the entire subtree of search_base
        search_scope="SUBTREE",
        # Fetch only distinguishedName and objectGUID attributes
        attributes=["distinguishedName", "objectGUID"],
    )
    json_str = ad_connection.response_to_json()
    ad_response = json.loads(json_str)
    org_units = list(map(itemgetter("attributes"), ad_response["entries"]))
    distinguished_names = map(itemgetter("distinguishedName"), org_units)
    parsed_names: Iterator[ParsedDN] = map(
        parse_distinguished_name, distinguished_names
    )
    guids = map(
        lambda guid_str: UUID(guid_str.strip("{}")),
        map(itemgetter("objectGUID"), org_units),
    )
    result = dict(zip(guids, parsed_names))
    # Assert that there were no collisions
    assert len(result) == len(ad_response["entries"])
    return result


def strip_users_and_computers(ad_tree: ADTree) -> ADTree:
    return {
        key: value
        for key, value in ad_tree.items()
        if value.parsed_dn[-1] not in ("Brugere", "Computere", "Computer")
    }


def build_parent_map(ad_tree: ADTree) -> Dict[UUID, Optional[UUID]]:
    def naive_transpose(ad_tree: ADTree):
        return {value.parsed_dn: key for key, value in ad_tree.items()}

    mapping_tree = naive_transpose(ad_tree)

    return {
        key: mapping_tree.get(value.parsed_dn[:-1])  # type: ignore
        for key, value in ad_tree.items()
    }


def build_name_map(ad_tree: ADTree) -> Dict[UUID, str]:
    return {key: value.parsed_dn[-1] for key, value in ad_tree.items()}


def build_children_map(parent_map):
    """
    Build children map from parent map.

    Roots are assumed to have parent of :code:`None`

    Example:
        >>> parent_map = {1: None, 2: 1, 3: 2, 4: 1, 5: 2}
        >>> expected = {None: [1], 1: [2, 4], 2: [3, 5]}
        >>> build_children_map(parent_map) == expected
        True

    Args:
        parent_map: dict from id to parent id

    Returns:
        dict: A dictionary from parent-id to list of children
    """
    children_map = defaultdict(list)
    for idd, parent in parent_map.items():
        children_map[parent].append(idd)
    children_map = dict(children_map)
    return children_map


def recursive_build_tree(children_map, entry=None):
    """Recursively build tree structure from children map.

    Roots are assumed to be parents of :code:`None`.

    Example:
        >>> children_map = {None: [1], 1: [2, 4], 2: [3, 5]}
        >>> expected = {1: {2: {3: {}, 5: {}}, 4: {}}}
        >>> recursive_build_tree(children_map) == expected
        True

    Args:
        children_map: dict from parent-id to list of children
        entry: key from children_map to build subtree for

    Returns:
        dict: Tree structure
    """
    output = {}
    for root in children_map.get(entry, []):
        output[root] = {}
        for entry in children_map.get(root, []):
            output[root][entry] = recursive_build_tree(children_map, entry)
    return output


def build_tree(parent_map):
    """
    >>> parent_map = {1: None, 2: 1, 3: 2, 4: 1, 5: 2}
    >>> expected = {1: {2: {3: {}, 5: {}}, 4: {}}}
    >>> build_tree(parent_map) == expected
    True
    """
    children_map = build_children_map(parent_map)
    return recursive_build_tree(children_map)


def construct_org_unit(
    parent_map,
    name_map,
    uuid: UUID,
    org_unit_type_uuid: UUID,
    org_unit_level_uuid: UUID,
) -> OrganisationUnit:
    parent_uuid = parent_map[uuid]
    name = name_map[uuid]

    return OrganisationUnit.from_simplified_fields(
        uuid=uuid,
        parent_uuid=parent_uuid,
        user_key=name,
        name=name,
        org_unit_type_uuid=org_unit_type_uuid,
        # MO does *not* require specifying `org_unit_level_uuid` - but `ra-data-models`
        # *does* require it (which should be changed to reflect MO.)
        org_unit_level_uuid=org_unit_level_uuid,
        from_date=str(date.today()),
    )


def build_model_map(
    ad_tree: ADTree, org_unit_type_uuid: UUID, org_unit_level_uuid: UUID
) -> MOOrganisationUnitMap:
    parent_map = build_parent_map(ad_tree)
    name_map = build_name_map(ad_tree)
    org_units = set(ad_tree.keys())
    return {
        key: construct_org_unit(
            parent_map, name_map, key, org_unit_type_uuid, org_unit_level_uuid
        )
        for key in org_units
    }


def print_tree(tree, formatter):
    tr = LeftAligned()
    tr.traverse.get_text = lambda node: formatter(node[0])
    print(tr(tree))


def tree_visitor(
    tree: OrgTree,
    yield_func: Callable[[UUID, int], CallableReturnType],
    level: int = 1,
) -> Iterator[CallableReturnType]:
    for name, children in tree.items():
        yield yield_func(name, level)
        yield from tree_visitor(children, yield_func, level + 1)


def build_ad_tree(settings: dict) -> ADTree:
    ad_connection = configure_ad_connection(settings)
    with ad_connection:
        ad_tree = load_ad_tree(settings, ad_connection)
        ad_tree = strip_users_and_computers(ad_tree)
        return ad_tree


def build_org_tree(ad_tree: ADTree):
    parent_map = build_parent_map(ad_tree)
    tree = build_tree(parent_map)
    return tree


def build_model_layers(tree, model_map: MOOrganisationUnitMap):
    def visitor(uuid: UUID, level: int) -> Tuple[int, OrganisationUnit]:
        return level, model_map[uuid]

    model_tree = list(tree_visitor(tree, visitor))
    model_layers = groupby(sorted(model_tree, key=itemgetter(0)), itemgetter(0))
    layers = [
        list(map(itemgetter(1), model_layer)) for level, model_layer in model_layers
    ]

    return layers


def dump_csv(ad_tree: ADTree, writable: IO):
    writer = csv.DictWriter(
        writable, delimiter=";", fieldnames=["UUID", "DistinguishedName"]
    )
    writer.writeheader()
    writer.writerows(
        {"UUID": key, "DistinguishedName": value.dn} for key, value in ad_tree.items()
    )


@click.command()
def print_adtree():
    settings = get_ldap_settings()
    ad_tree = build_ad_tree(settings)
    tree = build_org_tree(ad_tree)
    model_map = build_model_map(ad_tree)

    def node_formatter(uuid: UUID) -> str:
        return model_map[uuid].name

    print_tree(tree, node_formatter)


@click.command()
@click.option(
    "--org-unit-type-uuid",
    required=True,
    type=click.UUID,
    help="UUID of `org_unit_type` to use when creating organisation units",
)
@click.option(
    "--org-unit-level-uuid",
    required=True,
    type=click.UUID,
    help="UUID of `org_unit_level` to use when creating organisation units",
)
@click.option(
    "--csv-path",
    default="./adtreesync.csv",
    type=click.Path(),
    help="Path of CSV file to output",
)
@async_to_sync
async def upload_adtree(
    org_unit_type_uuid: UUID,
    org_unit_level_uuid: UUID,
    csv_path: str,
):
    settings = get_ldap_settings()
    ad_tree = build_ad_tree(settings)
    dump_csv(ad_tree, open(csv_path, "w"))
    tree = build_org_tree(ad_tree)
    model_map = build_model_map(ad_tree, org_unit_type_uuid, org_unit_level_uuid)
    layers = build_model_layers(tree, model_map)

    # TODO: Root should be renamed 'Administrativ organisation'

    mora_settings = get_mora_settings()
    client = ModelClient(
        base_url="http://localhost:5000",
        client_id=mora_settings.client_id,
        client_secret=mora_settings.client_secret,
        auth_server=mora_settings.auth_server,
        auth_realm=mora_settings.auth_realm,
    )
    async with client:
        for layer in layers:
            await client.upload(layer)


if __name__ == "__main__":
    upload_adtree()
