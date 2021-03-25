import asyncio
import itertools
import sys
import typing
from queue import Queue

import aiohttp
import click
import requests
import tqdm
from tqdm.asyncio import tqdm
from more_itertools import flatten
from mox_helpers.utils import async_to_sync
from exporters.utils.load_settings import load_settings


class SubtreeDeleter:
    def __init__(self, session, subtree_uuid):
        self.session = session
        settings = load_settings()
        self.mora_base = settings.get("mora.base")
        self.mox_base = settings.get("mox.base")

    async def get_org_uuid(self):
        async with self.session.get(f"{self.mora_base}/service/o/") as r:
            r.raise_for_status()
            r = await r.json()
            return r[0]["uuid"]

    async def get_tree(self, org_uuid):
        async with self.session.get(
            f"{self.mora_base}/service/o/{org_uuid}/ou/tree"
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def get_associated_org_func(self, org_unit_uuid):
        async with self.session.get(
            f"{self.mox_base}/organisation/organisationfunktion?tilknyttedeenheder={org_unit_uuid}"
        ) as r:
            r.raise_for_status()
            r = await r.json()
            return r["results"][0]

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

    async def get_associated_org_funcs(self, unit_uuids: typing.List[str]):
        org_func_uuids = map(self.get_associated_org_func, unit_uuids)
        return await tqdm.gather(org_func_uuids)

    async def deleter(self, path, uuid):
        url = f"{self.mox_base}/{path}/{uuid}"
        async with self.session.delete(url) as r:
            r.raise_for_status()
            return await r.json()

    async def delete_from_lora(self, uuids, path):
        tasks = [self.deleter(path, uuid) for uuid in uuids]
        await tqdm.gather(tasks)

    async def run(self, subtree_uuid, delete_functions):
        org_uuid = await self.get_org_uuid()
        tree = await self.get_tree(org_uuid)
        subtree = self.find_subtree(subtree_uuid, tree)

        print("Deleting subtree for {}".format(subtree_uuid))
        unit_uuids = self.get_tree_uuids(subtree)
        await self.delete_from_lora(unit_uuids, "organisation/organisationenhed")

        if delete_functions:
            print("Deleting associated org functions for subtree".format(subtree_uuid))
            org_func_uuids = await self.get_associated_org_funcs(unit_uuids)
            await self.delete_from_lora(
                flatten(org_func_uuids), "organisation/organisationfunktion"
            )

        print("Done")


@async_to_sync
async def subtreedeleter_helper(
    org_unit_uuid: str, delete_functions: bool = False
) -> None:
    settings = load_settings()
    api_token = settings.get("crontab.SAML_TOKEN")
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        session.headers.update({"session": api_token})
        deleter = SubtreeDeleter(session, org_unit_uuid)
        await deleter.run(org_unit_uuid, delete_functions)


@click.command()
@click.argument("org_unit_uuid")
@click.option(
    "--delete-functions",
    default=False,
    help="Delete all organisational functions associated with units in the subtree",
)
def main(org_unit_uuid, delete_functions):
    subtreedeleter_helper(org_unit_uuid, delete_functions)


if __name__ == "__main__":
    main()
