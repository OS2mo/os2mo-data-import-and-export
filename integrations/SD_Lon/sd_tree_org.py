import asyncio
from functools import partial, wraps
from operator import itemgetter

import click
from anytree import Node, RenderTree

from integrations.SD_Lon.sd_common import sd_lookup_settings
from integrations.SD_Lon.SDConnector import SDConnector


def create_sd_connector():
    institution_identifier, sd_username, sd_password = sd_lookup_settings()
    return SDConnector(institution_identifier, sd_username, sd_password)


def async_to_sync(f):
    """Decorator to run an async function to completion.

    Example:

        @async_to_sync
        async def sleepy(seconds):
            await sleep(seconds)

        sleepy(5)

    Args:
        f (async function): The async function to wrap and make synchronous.

    Returns:
        :obj:`sync function`: The syncronhous function wrapping the async one.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(f(*args, **kwargs))
        return loop.run_until_complete(future)

    return wrapper


@click.command()
@async_to_sync
async def sd_tree_org():
    """Tool to print out the entire SD organization tree."""

    def build_parent_map(parent_map, department):
        uuid = department["DepartmentUUIDIdentifier"]
        if uuid in parent_map:
            return parent_map

        # Add ourselves to the parent map
        parent_map[uuid] = {
            "identifier": department["DepartmentIdentifier"],
            "level": department["DepartmentLevelIdentifier"],
            "parent": department.get("DepartmentReference", {}).get(
                "DepartmentUUIDIdentifier"
            ),
        }

        # Call recursively (if required)
        if "DepartmentReference" in department:
            parent_map = build_parent_map(parent_map, department["DepartmentReference"])
        return parent_map

    def find_children_uuids(tree, parent_uuid):
        children_uuids = [
            key for key, value in sorted(tree.items()) if value["parent"] == parent_uuid
        ]
        return children_uuids

    def build_any_tree(parent_map, root_uuid):
        def build_tree_node(uuid, parent=None):
            node = Node(department_name_map[uuid] + " (" + department_id_map[uuid] + ", " + uuid + ")", parent=parent)
            return node

        def build_tree(parent_node, parent_uuid):
            node_uuids = find_children_uuids(parent_map, parent_uuid)
            for node_uuid in node_uuids:
                node = build_tree_node(node_uuid, parent=parent_node)
                build_tree(node, node_uuid)

        root = build_tree_node(root_uuid)
        build_tree(root, root_uuid)
        return root

    sd_connector = create_sd_connector()

    # Fire our requests
    responses = await asyncio.gather(
        sd_connector.getDepartment(), sd_connector.getOrganization()
    )
    department_response, organization_response = responses
    # Pull out the data
    departments = department_response["Department"]
    organization = organization_response["Organization"]["DepartmentReference"]

    # Generate map from UUID to Name for Deparments
    department_name_map = dict(
        map(itemgetter("DepartmentUUIDIdentifier", "DepartmentName"), departments)
    )
    department_id_map = dict(
        map(itemgetter("DepartmentUUIDIdentifier", "DepartmentIdentifier"), departments)
    )

    # Build parent map
    parent_map = {}
    for department in organization:
        parent_map = build_parent_map(parent_map, department)

    # Find roots of the parent_map
    root_uuids = find_children_uuids(parent_map, None)

    # For each root, build an any-tree and print it
    trees = map(partial(build_any_tree, parent_map), root_uuids)
    for tree in trees:
        for pre, fill, node in RenderTree(tree):
            print("%s%s" % (pre, node.name))
        print()


@click.command()
@async_to_sync
async def department_identifier_list():
    sd_connector = create_sd_connector()
    department_response = await sd_connector.getDepartment()
    departments = department_response["Department"]
    from collections import Counter
    department_identifiers = Counter(
        map(itemgetter("DepartmentIdentifier"), departments)
    )
    for element, count in department_identifiers.most_common():
        if count == 1:
            break
        print(element, count)


if __name__ == "__main__":
    department_identifier_list()
    # sd_tree_org()
