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

logger = logging.getLogger('Exports')


@click.command()
@click.option('--dev', 'hostname', flag_value='https://os2mo-dev.holstebro.dk', help='https://os2mo-dev.holstebro.dk')
@click.option('--test', 'hostname', flag_value='https://os2mo-test.holstebro.dk', help='https://os2mo-test.holstebro.dk')
@click.option('--prod', 'hostname', flag_value='https://os2mo.holstebro.dk', help='https://os2mo.holstebro.dk')
def export_from_mo(hostname):
    threaded_speedup = False
    t = time.time()
    holstebro_logger.start_logging(SETTINGS['logging.holstebro.exporters_logfile'])

    mh = MoraHelper(hostname=hostname, export_ansi=False)

    logger.info(f"Reading organisation from: {hostname}")
    org = mh.read_organisation()

    # find Holstebro Kommune root uuid, if no uuid is specified
    roots = mh.read_top_units(org)
    for root in roots:
        if root['name'] == SETTINGS['municipality.name']:
            holstebro_uuid = root['uuid']

    filename = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    planorama_org = f"{filename}_{SETTINGS['exports.holstebro.planorama.org_filename']}"
    planorama_employee = f"{filename}_{SETTINGS['exports.holstebro.planorama.employee_filename']}"
    essens_lms_filename = f"{filename}_{SETTINGS['exports.holstebro.essenslms.filename']}"

    # TEST UUIDs
    itdig_uuid = '9f981b4d-66c3-4100-b800-000001480001'
    okit_uuid = '470ce14c-66c3-4100-ba00-0000014b0001'
    bufl_uuid = '4105e152-66c3-4100-8c00-0000014b0001'

    #holstebro_uuid = itdig_uuid
    #holstebro_uuid = okit_uuid
    #holstebro_uuid = bufl_uuid

    logger.info(f"Reading ou tree from {holstebro_uuid}")
    nodes = mh.read_ou_tree(holstebro_uuid)
    logger.info('Read nodes: {}s'.format(time.time() - t))

    if threaded_speedup:
        cq.pre_cache_users(mh)
        logger.info('Build cache: {}'.format(time.time() - t))

    logger.info(f"Exporting data to Planorama")
    hh.export_to_planorama(mh, nodes, planorama_org, planorama_employee)
    logger.info(f"{planorama_org}: {time.time() - t}")

    logger.info(f"Exporting data to EssensLMS")
    hh.export_to_essenslms(mh, nodes, essens_lms_filename)
    logger.info(f"{essens_lms_filename}: {time.time() - t}")


if __name__ == '__main__':
    export_from_mo()
