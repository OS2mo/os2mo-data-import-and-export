import json
import xml
from collections import defaultdict
from operator import itemgetter

import click
import xmltodict
from asciitree import LeftAligned


def dict_map(dicty, key_fun=None, value_fun=None, tuple_fun=None):
    """Map the dict values.

    Example:
        >>> dts = lambda dicty: sorted(dicty.items())
        >>> dicty = {1: 1, 2: 2, 3: 3}
        >>> dts(dict_map(dicty, value_fun=lambda value: value ** 2))
        [(1, 1), (2, 4), (3, 9)]
        >>> dts(dict_map(dicty, key_fun=lambda key: key + 2))
        [(3, 1), (4, 2), (5, 3)]
        >>> dts(dict_map(dicty, tuple_fun=lambda key, value: (key + 2, value ** 2)))
        [(3, 1), (4, 4), (5, 9)]

    Returns:
        dict: A dict where func has been applied to every value.
    """
    key_fun = key_fun or (lambda key: key)
    value_fun = value_fun or (lambda value: value)
    tuple_fun = tuple_fun or (lambda key, value: (key, value))
    return dict(
        tuple_fun(key_fun(key), value_fun(value)) for key, value in dicty.items()
    )


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


def dict_from_xmlfile(filename):
    def read_file(filename):
        with open(filename, "r") as input_file:
            data = input_file.read()
            return data

    def read_json_file(filename):
        return json.loads(read_file(filename))

    def write_json_file(filename, data):
        with open(filename, "w") as output_file:
            json.dump(data, output_file)

    # Read cache, if any
    cache_filename = filename + ".json"
    try:
        return read_json_file(cache_filename)
    # No cache, no problem, read xml source and produce cache
    except FileNotFoundError:
        xml_data = read_file(filename)
        data = xmltodict.parse(xml_data)
        write_json_file(cache_filename, data)
        return data


def print_tree(tree, print_map):
    tr = LeftAligned()
    tr.traverse.get_text = lambda node: print_map[node[0]]
    print(tr(tree))


@click.command()
@click.argument("filename", type=click.Path(exists=True))
def output_tree(filename):
    """Load organisation units from [FILENAME] and print org-tree."""
    # Load data and process it
    try:
        data = dict_from_xmlfile(filename)
    except xml.parsers.expat.ExpatError:
        raise click.ClickException("Provided file not in XML format")
    try:
        org_units = data["kmd"]["orgUnit"]
        parent_map = dict(map(itemgetter("@id", "parentOrgUnit"), org_units))
        print_map = dict_map(
            dict(map(itemgetter("@id", "longName"), org_units)),
            tuple_fun=lambda key, value: (key, value + " (" + key + ")"),
        )
    except KeyError:
        raise click.ClickException("Unexpected XML")

    tree = build_tree(parent_map)
    print_tree(tree, print_map)


if __name__ == "__main__":
    output_tree()
