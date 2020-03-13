import os
import queue
import threading
import time
from datetime import datetime, timedelta

import click
import requests
from anytree import Node, PreOrderIter

import common_queries as cq
import holstebro_helpers as hh
from os2mo_helpers.mora_helpers import MoraHelper


@click.command()
@click.option('--root', default=None, help='uuid of the root to be exported.')
@click.option('--threaded-speedup', default=False, help='Run in multithreaded mode')
@click.option('--dev', 'hostname', flag_value='https://os2mo-dev.holstebro.dk', help='https://os2mo-dev.holstebro.dk')
@click.option('--test', 'hostname', flag_value='https://os2mo-test.holstebro.dk', help='https://os2mo-test.holstebro.dk')
@click.option('--prod', 'hostname', flag_value='https://os2mo.holstebro.dk', help='https://os2mo.holstebro.dk')
@click.option('--api_token', envvar='SAML_TOKEN', default=None, required=True, help='SAML API Token')
def decorate_leaders(root, threaded_speedup, hostname, api_token):
    if api_token is None:
        raise NameError('Ugyldigt argument')

    mh = MoraHelper(hostname=hostname, export_ansi=False)

    org = mh.read_organisation()

    # find Holstebro Kommune root uuid, if no uuid is specified
    if root is None:
        roots = mh.read_top_units(org)
        for root in roots:
            if root['name'] == 'Holstebro Kommune':
                holstebro_uuid = root['uuid']
    else:
        holstebro_uuid = root

    nodes = mh.read_ou_tree(holstebro_uuid)

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    hh.update_org_with_hk_managers(mh, nodes)


if __name__ == '__main__':
    decorate_leaders()
