import json
from collections import defaultdict
from datetime import date
from itertools import groupby
from operator import itemgetter
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import TypeVar
from uuid import UUID

import click
from asciitree import LeftAligned
from ra_utils.async_to_sync import async_to_sync
from raclients.modelclient.mo import ModelClient
from ramodels.mo import OrganisationUnit

from .config import get_ldap_settings
from .ldap import configure_ad_connection

CallableReturnType = TypeVar("CallableReturnType")
OrgTree = Dict[UUID, Dict]

ORG_UNIT_TYPE_UUID = UUID("3a895052-dabb-4fad-8e45-3154330d0203")
# RADataModels must be changed to allow None for level in writing
ORG_UNIT_LEVEL_UUID = None  # UUID("00000000-0000-0000-0000-000000000000")


def parse_distinguished_name(dn: str) -> Tuple[str]:
    # Convert "\\," to "|"
    dn = dn.replace("\\,", "|")
    # Split on "," and replace "|" with ","
    split = [s.replace("|", ",") for s in dn.split(",")]
    # ["OU=Foo", "DC="Bar"] -> [("OU", "Foo"), ("DC", "Bar")]
    categorized = map(lambda s: s.split("="), split)
    # [("OU", "Foo"), ("DC", "Bar")] -> ["Foo"]
    relevant = [value for (category, value) in categorized if category == "OU"]
    return tuple(reversed(relevant))  # type: ignore


def load_ad_tree(settings, ad_connection) -> Dict[UUID, Tuple[str]]:
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
    parsed_names = map(parse_distinguished_name, distinguished_names)

    guids = map(
        lambda guid_str: UUID(guid_str.strip("{}")),
        map(itemgetter("objectGUID"), org_units),
    )
    result = dict(zip(guids, parsed_names))
    # Assert that there were no collisions
    assert len(result) == len(ad_response["entries"])
    return result


def strip_users_and_computers(
    ad_tree: Dict[UUID, Tuple[str]]
) -> Dict[UUID, Tuple[str]]:
    return {
        key: value
        for key, value in ad_tree.items()
        if value[-1] not in ("Brugere", "Computere", "Computer")
    }


def naive_transpose(ad_tree: Dict[UUID, Tuple[str]]) -> Dict[Tuple[str], UUID]:
    return {value: key for key, value in ad_tree.items()}


def build_parent_map(ad_tree: Dict[UUID, Tuple[str]]) -> Dict[UUID, Optional[UUID]]:
    mapping_tree = naive_transpose(ad_tree)
    return {key: mapping_tree.get(value[:-1]) for key, value in ad_tree.items()}  # type: ignore


def build_name_map(ad_tree: Dict[UUID, Tuple[str]]) -> Dict[UUID, str]:
    return {key: value[-1] for key, value in ad_tree.items()}


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


def construct_org_unit(parent_map, name_map, uuid: UUID) -> OrganisationUnit:
    parent_uuid = parent_map[uuid]
    name = name_map[uuid]

    return OrganisationUnit.from_simplified_fields(
        uuid=uuid,
        parent_uuid=parent_uuid,
        user_key=name,
        name=name,
        org_unit_type_uuid=ORG_UNIT_TYPE_UUID,
        org_unit_level_uuid=ORG_UNIT_LEVEL_UUID,
        from_date=str(date.today()),
    )


def build_model_map(ad_tree) -> Dict[UUID, OrganisationUnit]:
    parent_map = build_parent_map(ad_tree)
    name_map = build_name_map(ad_tree)
    org_units = set(ad_tree.keys())

    return {key: construct_org_unit(parent_map, name_map, key) for key in org_units}


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


def build_ad_tree(settings: dict):
    ad_connection = configure_ad_connection(settings)
    with ad_connection:
        ad_tree = load_ad_tree(settings, ad_connection)
        ad_tree = strip_users_and_computers(ad_tree)
        return ad_tree


def build_org_tree(ad_tree):
    parent_map = build_parent_map(ad_tree)
    tree = build_tree(parent_map)
    return tree


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
@async_to_sync
async def upload_adtree():
    settings = get_ldap_settings()
    ad_tree = build_ad_tree(settings)
    tree = build_org_tree(ad_tree)
    model_map = build_model_map(ad_tree)

    def visitor(uuid: UUID, level: int) -> Tuple[int, OrganisationUnit]:
        return level, model_map[uuid]

    model_tree = list(tree_visitor(tree, visitor))
    # for level, org_unit in model_tree:
    #   print("  " * (level - 1), org_unit.name)

    model_layers = groupby(sorted(model_tree, key=itemgetter(0)), itemgetter(0))
    layers = [
        list(map(itemgetter(1), model_layer)) for level, model_layer in model_layers
    ]
    # level = 0
    # for layer in layers:
    #    for entity in layer:
    #        print("  " * level, entity)
    #    level += 1

    # TODO: Root should be renamed 'Administrativ organisation'

    client = ModelClient(
        base_url=settings.fastramqpi.mo_url,
        client_id=settings.fastramqpi.client_id,
        client_secret=settings.fastramqpi.client_secret.get_secret_value(),
        auth_server=settings.fastramqpi.auth_server,
        auth_realm=settings.fastramqpi.auth_realm,
    )
    async with client:
        for layer in layers:
            await client.upload(layer)


if __name__ == "__main__":
    upload_adtree()
