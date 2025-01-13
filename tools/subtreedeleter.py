import asyncio
import urllib.parse
from queue import Queue
from typing import List
from typing import Optional

import aiohttp
import click
from fastramqpi.ra_utils.headers import TokenSettings
from fastramqpi.ra_utils.load_settings import load_settings
from more_itertools import flatten
from more_itertools import one
from mox_helpers.utils import async_to_sync
from tqdm.asyncio import tqdm

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
    "Owner",  # Function name is in english for some reason.
]


class SubtreeDeleter:
    def __init__(self, session, connections: int = 4):
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
        self, org_unit_uuid: str, funktionsnavn: Optional[str] = None
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
    def find_subtree(subtree_uuid: str, trees: List[dict]):
        queue: Queue = Queue()
        for tree in trees:
            queue.put(tree)

        while not queue.empty():
            tree = queue.get()
            if tree["uuid"] == subtree_uuid:
                return tree
            children: Optional[List[dict]] = tree.get("children")
            if children:
                for child in children:
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
                *(self.get_associated_org_func(uuid) for uuid in unit_uuids)
            )
        return await tqdm.gather(
            *(
                self.get_associated_org_func(uuid, f)
                for uuid in unit_uuids
                for f in funktionsnavne
            )
        )

    async def deleter(self, path, uuid):
        url = f"{self.mox_base}/{path}/{uuid}"
        async with self.sem:
            async with self.session.delete(url) as r:
                r.raise_for_status()
                return await r.json()

    async def delete_from_lora(self, uuids, path):
        return await tqdm.gather(*(self.deleter(path, uuid) for uuid in uuids))

    async def run(
        self,
        subtree_uuid: str,
        delete_functions: bool,
        keep_functions: List[str] = [],
        delete_subtree: bool = False,
    ):
        org_uuid = await self.get_org_uuid()
        tree = await self.get_tree(org_uuid)
        subtree = self.find_subtree(subtree_uuid, tree)

        unit_uuids = self.get_tree_uuids(subtree)
        if delete_subtree:
            print("Deleting subtree for {}".format(subtree_uuid))
            await self.delete_from_lora(unit_uuids, "organisation/organisationenhed")
            print(f"Successfully deleted subtree: {subtree_uuid}")

        if delete_functions:
            print(
                "Deleting associated org functions for subtree {}".format(subtree_uuid)
            )
            funktionsnavne = []
            if keep_functions:
                funktionsnavne = [
                    f for f in all_functionnames if f not in keep_functions
                ]
            org_func_uuids = await self.get_associated_org_funcs(
                unit_uuids, funktionsnavne=funktionsnavne
            )
            await self.delete_from_lora(
                flatten(org_func_uuids), "organisation/organisationfunktion"
            )

        print("Done")


@async_to_sync
async def subtreedeleter_helper(
    org_unit_uuid: str,
    delete_functions: bool = False,
    keep_functions: List[str] = [],
    delete_subtree: bool = False,
    connections: int = 4,
) -> None:
    token_settings = TokenSettings()  # type: ignore
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        session.headers.update(token_settings.get_headers())
        deleter = SubtreeDeleter(session, connections=connections)
        await deleter.run(
            org_unit_uuid,
            delete_functions,
            keep_functions=keep_functions,
            delete_subtree=delete_subtree,
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
    "--delete-subtree",
    default=False,
    help="Specify whether to delete subtrees. If set, all org_units within the subtree will be deleted",
)
@click.option(
    "--connections",
    default=4,
    help="The amount of concurrent requests made to OS2mo",
)
def main(org_unit_uuid, delete_functions, keep, delete_subtree, connections):
    """Delete an organisational unit and all units below.

    Given the uuid of an org_unit this will delete the unit and all units below it.
    Optionally also deletes organisation functions such as engagements, KLE and addresses.
    Optionally deletes the organisation unit and its subtrees. Must be set "True" for deletion, if wished to delete,
    otherwise "False" to keep subtrees, when calling the function.
    To delete all organisation functions, except certain type(s), add them with --keep.
    Example:
        python3 tools/subtreedeleter.py --org-unit-uuid=c9b4c61f-1d38-5f6a-2c9e-d001e7cf6bd0 --delete-functions --keep=Leder --keep=KLE --delete-subtree=True
    """
    subtreedeleter_helper(
        org_unit_uuid,
        delete_functions,
        keep_functions=keep,
        delete_subtree=delete_subtree,
        connections=connections,
    )


if __name__ == "__main__":
    main()
