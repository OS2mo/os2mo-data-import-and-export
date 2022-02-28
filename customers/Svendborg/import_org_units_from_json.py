import asyncio
import json
from datetime import date
from typing import Any
from typing import Dict
from typing import List
from uuid import UUID

from pydantic import AnyHttpUrl
from pydantic import parse_obj_as
from raclients.modelclient.mo import ModelClient
from ramodels.mo import OrganisationUnit

# TODO: get from CLI arg
ORG_UNIT_TYPE_UUID = UUID("2b41cf13-f29c-4f47-b019-8600b73a9182")

# TODO: get from CLI arg
PATH = "/home/mrw/Downloads/SVK_OU_Dump.json"


class OrgUnitTreeNode:
    def __init__(self, dn: str, uuid: UUID):
        self.dn = dn
        self.uuid = uuid
        self.parent_uuid = None
        self.path = self._parse_dn(dn)

    def __gt__(self, other):
        return self.path > other.path

    def as_mo_model(self):
        name = self.path[-1]
        return OrganisationUnit.from_simplified_fields(
            uuid=self.uuid,
            user_key=name,
            name=name,
            parent_uuid=self.parent_uuid,
            org_unit_type_uuid=ORG_UNIT_TYPE_UUID,
            org_unit_level_uuid=ORG_UNIT_TYPE_UUID,
            from_date=str(date.today()),
        )

    def _parse_dn(self, dn: str) -> List[str]:
        # Convert "\\," to "|"
        dn = dn.replace("\\,", "|")
        # Split on "," and replace "|" with ","
        split = [s.replace("|", ",") for s in dn.split(",")]
        # ["OU=Foo", "DC="Bar"] -> [("OU", "Foo"), ("DC", "Bar")]
        categorized = map(lambda s: s.split("="), split)
        # [("OU", "Foo"), ("DC", "Bar")] -> ["Foo"]
        relevant = [value for (category, value) in categorized if category == "OU"]
        return relevant[::-1]


class OrgUnitTree:
    @classmethod
    def from_json(cls, path: str, encoding: str = "utf-8-sig"):
        with open(path, encoding=encoding) as input_stream:
            nodes = [
                OrgUnitTreeNode(elem["DistinguishedName"], elem["ObjectGuid"])
                for elem in json.load(input_stream)
            ]
            return cls(nodes)

    def __init__(self, nodes: List[OrgUnitTreeNode]):
        self.root = self._get_tree(sorted(nodes))  # type: ignore

    def as_mo_models(self) -> List[OrganisationUnit]:
        accum = []

        def _visit(node):
            for key, value in node.items():
                if key == "_node":
                    accum.append(value.as_mo_model())
                # Recurse
                if isinstance(value, dict):
                    _visit(value)

        _visit(self.root)

        return accum

    async def post_to_mo(self, using: ModelClient):
        async with using:
            await using.upload(self.as_mo_models())

    def _get_tree(self, nodes: List[OrgUnitTreeNode]):
        def _set_parent_uuid(node, parent=None):
            for key, value in node.items():
                if key == "_node":
                    parent_node = parent.get("_node")
                    if parent_node:
                        value.parent_uuid = parent_node.uuid
                # Recurse
                if isinstance(value, dict):
                    _set_parent_uuid(value, node)

        tree: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            if node.path[-1] in ("Brugere", "Computere", "Computer"):
                continue
            curr_tree = tree
            for key in node.path:
                if key not in curr_tree:
                    curr_tree[key] = {"_node": node}
                curr_tree = curr_tree[key]

        # Start "set parent UUID" from root of tree
        _set_parent_uuid(tree, parent=None)

        return tree


async def test():
    # TODO: get "client_id", "client_secret", etc. from settings.json or CLI args
    client = ModelClient(
        base_url="http://localhost:5000",
        client_id="dipex",
        client_secret="603f1c82-d012-4d04-9382-dbe659c533fb",
        auth_server=parse_obj_as(AnyHttpUrl, "http://localhost:8081/auth"),
        auth_realm="mo",
        force=True,
    )
    tree = OrgUnitTree.from_json(PATH)
    await tree.post_to_mo(using=client)


def view():
    tree = OrgUnitTree.from_json(PATH)

    def visit(node, indent=0):
        for key, value in node.items():
            if key != "_node":
                print("\t" * indent, key)
            # Recurse
            if isinstance(value, dict):
                visit(value, indent=indent + 1)

    visit(tree.root)


view()
asyncio.run(test())
