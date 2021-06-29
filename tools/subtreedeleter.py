import asyncio
import itertools
import sys
import typing
import urllib.parse
from queue import Queue
from typing import List

import aiohttp
import click
import requests
import tqdm
from more_itertools import flatten, one
from mox_helpers.utils import async_to_sync
from tqdm.asyncio import tqdm

from ra_utils.load_settings import load_settings


all_functionnames = [
    "Engagement",
    "Leder",
    "Addresse",
    "Tilknytning",
    "Rolle",
    "KLE",
    "Relateret Enhed",
    "IT-system",
    "Orlov",
]


class SubtreeDeleter:
    def __init__(self, session, subtree_uuid, connections: int = 4):
        self.session = session
        settings = load_settings()
        self.mora_base = settings.get("mora.base")
        self.mox_base = settings.get("mox.base")
        self.sem = asyncio.Semaphore(connections)

    async def get_org_uuid(self):
        async with self.session.get(f"{self.mora_base}/service/o/") as r:
            r.raise_for_status()
            r = await r.json()
            return one(r)["uuid"]

    async def get_tree(self, org_uuid):
        async with self.session.get(
            f"{self.mora_base}/service/o/{org_uuid}/ou/tree"
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def get_associated_org_func(
        self, org_unit_uuid: str, funktionsnavn: str = None
    ):
        url = f"{self.mox_base}/organisation/organisationfunktion?tilknyttedeenheder={org_unit_uuid}&virkningfra=-infinity&virkningtil=infinity"
        if funktionsnavn:
            funktionsnavn = urllib.parse.quote(funktionsnavn)
            url += f"&funktionsnavn={funktionsnavn}"
        async with self.session.get(url) as r:
            r.raise_for_status()
            r = await r.json()
            return one(r["results"])

    @staticmethod
    def find_subtree(subtree_uuid: str, trees: typing.List[dict]):
        queue = Queue()
        for tree in trees:
            queue.put(tree)

        while not queue.empty():
            tree = queue.get()
            if tree["uuid"] == subtree_uuid:
                return tree
            if tree.get("children"):
                for child in tree.get("children"):
                    queue.put(child)

        raise click.ClickException("{} not found".format(subtree_uuid))

    def get_tree_uuids(self, tree: dict):
        uuids = [tree["uuid"]]
        children = tree.get("children")
        if children:
            for subtree in tree["children"]:
                uuids += self.get_tree_uuids(subtree)
        return uuids

    async def get_associated_org_funcs(
        self, unit_uuids: List[str], funktionsnavne: List[str] = []
    ):
        if funktionsnavne == []:
            return await tqdm.gather(
                self.get_associated_org_func(uuid) for uuid in unit_uuids
            )
        return await tqdm.gather(
            self.get_associated_org_func(uuid, f)
            for uuid in unit_uuids
            for f in funktionsnavne
        )

    async def deleter(self, path, uuid):
        url = f"{self.mox_base}/{path}/{uuid}"
        async with self.sem:
            async with self.session.delete(url) as r:
                r.raise_for_status()
                return await r.json()

    async def delete_from_lora(self, uuids, path):
        return await tqdm.gather(self.deleter(path, uuid) for uuid in uuids)

    async def run(
        self, subtree_uuid: str, delete_functions: bool, keep_functions: List[str] = []
    ):
        org_uuid = await self.get_org_uuid()
        tree = await self.get_tree(org_uuid)
        subtree = self.find_subtree(subtree_uuid, tree)

        print("Deleting subtree for {}".format(subtree_uuid))
        unit_uuids = self.get_tree_uuids(subtree)
        await self.delete_from_lora(unit_uuids, "organisation/organisationenhed")

        if delete_functions:
            print("Deleting associated org functions for subtree".format(subtree_uuid))
            funktionsnavne = []
            if keep_functions:
                funktionsnavne = [f for f in all_functionnames if f not in keep_functions]
            org_func_uuids = await self.get_associated_org_funcs(
                unit_uuids, funktionsnavne=funktionsnavne
            )
            await self.delete_from_lora(
                flatten(org_func_uuids), "organisation/organisationfunktion"
            )

        print("Done")


@async_to_sync
async def subtreedeleter_helper(
    org_unit_uuid: str, delete_functions: bool = False, keep_functions: List[str] = [], connections: int = 4
) -> None:
    settings = load_settings()
    api_token = settings.get("crontab.SAML_TOKEN")
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        session.headers.update({"session": api_token})
        deleter = SubtreeDeleter(session, org_unit_uuid, connections=connections)
        await deleter.run(
            org_unit_uuid, delete_functions, keep_functions=keep_functions
        )


@click.command()
@click.option(
    "--org-unit-uuid",
    required=True,
    help="Delete org_unit and all units in the tree below",
)
@click.option(
    "--delete-functions",
    is_flag=True,
    help="Delete all organisational functions associated with units in the subtree",
)
@click.option(
    "--keep",
    type=click.Choice(all_functionnames, case_sensitive=False),
    multiple=True,
    help="List of functions that should not be deleted",
)
@click.option(
    "--connections",
    default=4,
    help="The amount of concurrent requests made to OS2mo",
)
def main(org_unit_uuid, delete_functions, keep, connections):
    """Delete an organisational unit and all units below.

    Given the uuid of an org_unit this will delete the unit and all units below it. Optionally also deletes organisationfunctions such as engagements, KLE and addresses.
    To delete all organisationfunctions except certain type(s) add them with --keep.
    Example:
        venv/bin/python tools/subtreedeleter.py --org-unit-uuid=c9b4c61f-1d38-5f6a-2c9e-d001e7cf6bd0 --delete-functions --keep=Leder --keep=KLE
    """
    subtreedeleter_helper(org_unit_uuid, delete_functions, keep_functions=keep, connections=connections)


if __name__ == "__main__":
    main()
