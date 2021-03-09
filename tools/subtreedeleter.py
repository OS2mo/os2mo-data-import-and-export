import itertools

import sys
from queue import Queue

import requests
import click
import typing


class SubtreeDeleter:
    def __init__(self, api_token, subtree_uuid):
        self.session = requests.Session()
        self.session.headers.update({'session': api_token})

    def get_org_uuid(self):
        r = self.session.get('http://localhost:5000/service/o/')
        r.raise_for_status()
        return r.json()[0]['uuid']

    def get_tree(self, org_uuid):
        r = self.session.get(
            'http://localhost:5000/service/o/{}/ou/tree'.format(org_uuid)
        )
        r.raise_for_status()
        return r.json()

    def get_associated_org_func(self, org_unit_uuid):
        r = self.session.get(
            'http://localhost:8080/organisation/organisationfunktion?tilknyttedeenheder={}'.format(
                org_unit_uuid
            )
        )
        r.raise_for_status()
        return r.json()['results'][0]

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

    def get_associated_org_funcs(self, unit_uuids: typing.List[str]):
        org_func_uuids = map(self.get_associated_org_func, unit_uuids)
        return itertools.chain(*org_func_uuids)

    def delete_from_lora(self, uuids, path):
        url = "http://localhost:8080/{}/{}"
        for uuid in uuids:
            r = self.session.delete(url.format(path, uuid))
            r.raise_for_status()

    def run(self, subtree_uuid, delete_functions):
        org_uuid = self.get_org_uuid()
        tree = self.get_tree(org_uuid)
        subtree = self.find_subtree(subtree_uuid, tree)

        print('Deleting subtree for {}'.format(subtree_uuid))
        unit_uuids = self.get_tree_uuids(subtree)
        self.delete_from_lora(unit_uuids, 'organisation/organisationenhed')

        if delete_functions:
            print('Deleting associated org functions for subtree'.format(subtree_uuid))
            org_func_uuids = self.get_associated_org_funcs(unit_uuids)
            self.delete_from_lora(org_func_uuids, 'organisation/organisationfunktion')

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
    deleter = SubtreeDeleter(api_token, org_unit_uuid)
    deleter.run(org_unit_uuid, delete_functions)


if __name__ == '__main__':
    main()
