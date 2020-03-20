# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
"""
Holstebro Kommune specific queries into MO.
"""
import json
import logging
import os
import pathlib
import queue
import threading
import time
import string

from datetime import datetime, timedelta

import click
import requests
from anytree import Node, PreOrderIter

import common_queries as cq
import holstebro_helpers as hh
import holstebro_logger
from os2mo_helpers.mora_helpers import MoraHelper

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

logger = logging.getLogger('LederHierarki')


@click.command()
@click.option('--hostname', envvar='MORA_BASE', default=None, required=True, help='MO hostname')
@click.option('--api_token', envvar='SAML_TOKEN', default=None, required=True, help='SAML API Token')
def decorate_leaders(hostname, api_token):
    holstebro_logger.start_logging(SETTINGS['logging.holstebro.leaders_logfile'])

    t = time.time()

    if api_token is None:
        raise NameError('Ugyldigt argument')

    mh = MoraHelper(hostname=hostname, export_ansi=False)

    logger.info(f"Reading organisation from: {hostname}")
    org = mh.read_organisation()

    # find Holstebro Kommune root uuid, if no uuid is specified
    roots = mh.read_top_units(org)
    for root in roots:
        if root['name'] == SETTINGS['municipality.name']:
            holstebro_uuid = root['uuid']

    nodes = mh.read_ou_tree(holstebro_uuid)

    print('Read nodes: {}s'.format(time.time() - t))

    hh.update_org_with_hk_managers(mh, nodes)


if __name__ == '__main__':
    decorate_leaders()
