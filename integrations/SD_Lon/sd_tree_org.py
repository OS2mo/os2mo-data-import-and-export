import asyncio
import datetime
from functools import partial, wraps
from operator import itemgetter
from uuid import UUID

import aiohttp
import click
from anytree import Node, RenderTree
from xmltodict import parse

from integrations.SD_Lon.sd_common import sd_lookup_settings

xmlparse = partial(parse, dict_constructor=dict)


class SDAPIException(Exception):
    pass


def is_uuid(string: str) -> bool:
    try:
        UUID(string)
        return True
    except ValueError:
        return False


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


def today():
    today = datetime.date.today()
    return today


class SDConnector:
    def __init__(
        self,
        institution_identifier,
        sd_username,
        sd_password,
        sd_base_url="https://service.sd.dk/sdws/",
    ):
        self.institution_identifier = institution_identifier
        self.auth = aiohttp.BasicAuth(sd_username, sd_password)
        self.base_url = sd_base_url

    @staticmethod
    def create():
        institution_identifier, sd_username, sd_password = sd_lookup_settings()
        return SDConnector(institution_identifier, sd_username, sd_password)

    def _enrich_with_institution(self, params):
        if is_uuid(self.institution_identifier):
            params["InstitutionUUIDIdentifier"] = self.institution_identifier
        else:
            params["InstitutionIdentifier"] = self.institution_identifier
        return params

    def _enrich_with_department(self, params, department_identifier=None):
        if department_identifier is None:
            return params
        if is_uuid(department_identifier):
            params["DepartmentUUIDIdentifier"] = department_identifier
        else:
            params["DepartmentIdentifier"] = department_identifier
        return params

    def _enrich_with_dates(self, params, start_date=None, end_date=None):
        start_date = start_date or today()
        end_date = end_date or today()
        params.update(
            {
                "ActivationDate": start_date.strftime("%d.%m.%Y"),
                "DeactivationDate": end_date.strftime("%d.%m.%Y"),
            }
        )
        return params

    async def _send_request_xml(self, url, params):
        """Fire a requests against SD.

        Utilizes _sd_request to fire the actual request, which in turn utilize
        _sd_lookup_cache for caching.
        """
        # logger.info("Retrieve: {}".format(url))
        # logger.debug("Params: {}".format(params))

        full_url = self.base_url + url

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(full_url, auth=self.auth, params=params) as response:
                # We always expect SD to return UTF8 encoded XML
                assert response.headers["Content-Type"] == "text/xml;charset=UTF-8"
                return await response.text()

    async def _send_request_json(self, url, params):
        xml_response = await self._send_request_xml(url, params)
        dict_response = xmlparse(xml_response)
        if "Envelope" in dict_response:
            raise SDAPIException(dict_response["Envelope"])
        return dict_response

    async def getOrganization(
        self,
        start_date=None,
        end_date=None,
    ):
        params = {
            "UUIDIndicator": "true",
        }
        params = self._enrich_with_dates(params, start_date, end_date)
        params = self._enrich_with_institution(params)
        url = "GetOrganization20111201"
        dict_response = await self._send_request_json(url, params)
        return dict_response[url]

    async def getDepartment(
        self,
        department_identifier=None,
        department_level_identifier=None,
        start_date=None,
        end_date=None,
    ):
        params = {
            "ContactInformationIndicator": "true",
            "DepartmentNameIndicator": "true",
            "EmploymentDepartmentIndicator": "false",
            "PostalAddressIndicator": "true",
            "ProductionUnitIndicator": "true",
            "UUIDIndicator": "true",
        }
        if department_level_identifier:
            params["DepartmentLevelIdentifier"] = department_level_identifier
        params = self._enrich_with_dates(params, start_date, end_date)
        params = self._enrich_with_department(params, department_identifier)
        params = self._enrich_with_institution(params)

        url = "GetDepartment20111201"
        dict_response = await self._send_request_json(url, params)
        return dict_response[url]


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
            node = Node(department_map[uuid] + " (" + uuid + ")", parent=parent)
            return node

        def build_tree(parent_node, parent_uuid):
            node_uuids = find_children_uuids(parent_map, parent_uuid)
            for node_uuid in node_uuids:
                node = build_tree_node(node_uuid, parent=parent_node)
                build_tree(node, node_uuid)

        root = build_tree_node(root_uuid)
        build_tree(root, root_uuid)
        return root

    sd_connector = SDConnector.create()

    # Fire our requests
    responses = await asyncio.gather(
        sd_connector.getDepartment(), sd_connector.getOrganization()
    )
    department_response, organization_response = responses
    # Pull out the data
    departments = department_response["Department"]
    organization = organization_response["Organization"]["DepartmentReference"]

    # Generate map from UUID to Name for Deparments
    department_map = dict(
        map(itemgetter("DepartmentUUIDIdentifier", "DepartmentName"), departments)
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


if __name__ == "__main__":
    sd_tree_org()
