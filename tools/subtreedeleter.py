import asyncio
import itertools
import tqdm
import sys
from queue import Queue
from more_itertools import flatten
import requests
import click
import typing


class SubtreeDeleter:
    def __init__(self, session, subtree_uuid):
        self.session = session

    async def get_org_uuid(self):
        async with self.session.get('http://localhost:5000/service/o/') as r:
            r.raise_for_status()
            r = await r.json()
            return r[0]['uuid']

    async def get_tree(self, org_uuid):
        async with self.session.get(
            'http://localhost:5000/service/o/{}/ou/tree'.format(org_uuid)) as r:
            r.raise_for_status()
            return await r.json()

    async def get_associated_org_func(self, org_unit_uuid):
        async with self.session.get(
            'http://localhost:8080/organisation/organisationfunktion?tilknyttedeenheder={}'.format(
                org_unit_uuid
            )
        ) as r:
            r.raise_for_status()
            r = await r.json()
            return r['results'][0]

    @staticmethod
    def find_subtree(subtree_uuid: str, trees: typing.List[dict]):
        queue = Queue()
        for tree in trees:
            queue.put(tree)

        while not queue.empty():
            tree = queue.get()
            if tree['uuid'] == subtree_uuid:
                return tree
            if tree.get('children'):
                for child in tree.get('children'):
                    queue.put(child)

        raise click.ClickException('{} not found'.format(subtree_uuid))

    def get_tree_uuids(self, tree: dict):
        uuids = [tree['uuid']]
        children = tree.get('children')
        if children:
            for subtree in tree['children']:
                uuids += self.get_tree_uuids(subtree)
        return uuids

    async def get_associated_org_funcs(self, unit_uuids: typing.List[str]):
        org_func_uuids = map(self.get_associated_org_func, unit_uuids)
        return await asyncio.gather(*org_func_uuids)

    async def deleter(self, path, uuid):
            url = "http://localhost:8080/{}/{}"
            async with self.session.delete(url.format(path, uuid)) as r:
                r.raise_for_status()
                return await r.json()

            

    async def delete_from_lora(self, uuids, path):
        tasks = [self.deleter(path, uuid) for uuid in uuids]
        responses = [await f
                 for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        
            

    async def run(self, subtree_uuid, delete_functions):
        org_uuid = await self.get_org_uuid()
        tree = await self.get_tree(org_uuid)
        subtree = self.find_subtree(subtree_uuid, tree)

        print('Deleting subtree for {}'.format(subtree_uuid))
        unit_uuids = self.get_tree_uuids(subtree)
        await self.delete_from_lora(unit_uuids, 'organisation/organisationenhed')

        if delete_functions:
            print('Deleting associated org functions for subtree'.format(subtree_uuid))
            org_func_uuids = await self.get_associated_org_funcs(unit_uuids)
            await self.delete_from_lora(flatten(org_func_uuids), 'organisation/organisationfunktion')

        print('Done')


@click.command()
@click.argument('org_unit_uuid')
@click.option('--api-token')
@click.option(
    '--delete-functions',
    default=False,
    help="Delete all organisational functions associated with units in the subtree",
)
def main(api_token, org_unit_uuid, delete_functions):
    async def runner():
        async with aiohttp.ClientSession() as session:
            session.headers.update({'session': api_token})
            deleter = SubtreeDeleter(session, org_unit_uuid)
            deleter.run(org_unit_uuid, delete_functions)



if __name__ == '__main__':
    main()
