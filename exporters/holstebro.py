# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
"""
Holstebro Kommune specific queries into MO.
"""
import json
import os
import queue
import threading
import time

import click
import requests
from anytree import Node, PreOrderIter
from datetime import datetime, timedelta

import common_queries as cq
from os2mo_helpers.mora_helpers import MoraHelper
import holstebro_helpers as hh


@click.command()
@click.option('--root', default=None, help='uuid of the root to be exported.')
@click.option('--threaded-speedup', default=False, help='Run in multithreaded mode')
@click.option('--hostname', envvar='MORA_BASE', default=None, required=True, help='MO hostname')
@click.option('--api_token', envvar='SAML_TOKEN', default=None, required=True, help='SAML API Token')
# @click.option('--action', default='decorate_leaders', required=True, help='Which action should be performed?')
def export_from_mo(root, threaded_speedup, hostname, api_token):
    threaded_speedup = threaded_speedup
    t = time.time()

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

    itdig_uuid = '9f981b4d-66c3-4100-b800-000001480001'
    okit_uuid = '470ce14c-66c3-4100-ba00-0000014b0001'
    bufl_uuid = '4105e152-66c3-4100-8c00-0000014b0001'

    nodes = mh.read_ou_tree(holstebro_uuid)

    print('Read nodes: {}s'.format(time.time() - t))

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    # export_org_with_hk_managers(mh, nodes, 'hk_organisation.csv')

    hh.update_org_with_hk_managers(mh, nodes)

    """
    filename_org = 'planorama_org.csv'
    filename_persons = 'planorama_persons.csv'
    hh.export_to_planorama(mh, nodes, filename_org, filename_persons)
    print('planorama_org.csv: {}s'.format(time.time() - t))

    cq.export_orgs(mh, nodes, "hk_organisation.csv", False)
    print('hk_organisation.csv: {}s'.format(time.time() - t))
    """


if __name__ == '__main__':
    export_from_mo()
