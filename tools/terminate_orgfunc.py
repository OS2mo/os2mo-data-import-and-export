"""
Helper class to make a number of pre-defined queries into MO
"""
import json
import logging
import pathlib
import argparse
import datetime

from os2mo_helpers.mora_helpers import MoraHelper
from exporters.sql_export.lora_cache import LoraCache


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'terminate_orgfunc.log'

logger = logging.getLogger('terminate_orgfunc')

for name in logging.root.manager.loggerDict:
    if name in ('LoraCache',  'mora-helper', 'terminate_orgfunc'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())


def kill_it_connections(mh, lc):
    # denne terminerer alle brugeres it-forbindelser. Derfor er der ingen
    # grund til at se på brugeres uuider eller anden jøl.
    for uuid, itc in lc.it_connections.items():
        if itc[0]["user"] is None:
            continue

        date = datetime.datetime.now()
        terminate_datetime = date - datetime.timedelta(days=1)
        terminate_date = terminate_datetime.strftime('%Y-%m-%d')

        payload = {
            'type': 'it',
            'uuid': uuid,
            'validity': {'to': terminate_date}
        }

        logger.debug('Terminate payload: {}'.format(payload))
        response = mh._mo_post('details/terminate', payload)
        logger.debug('Terminate response: {} for user {}'.format(
                     response.text, itc[0]["user"]))


def kill_addresses(mh, lc):
    # denne terminerer alle brugeres addresser. Derfor er der ingen
    # grund til at se på brugeres uuider eller anden jøl.
    for uuid, addr in lc.addresses.items():
        if addr[0]["user"] is None:
            continue

        date = datetime.datetime.now()
        terminate_datetime = date - datetime.timedelta(days=1)
        terminate_date = terminate_datetime.strftime('%Y-%m-%d')

        payload = {
            'type': 'address',
            'uuid': uuid,
            'validity': {'to': terminate_date}
        }

        logger.debug('Terminate payload: {}'.format(payload))
        response = mh._mo_post('details/terminate', payload)
        logger.debug('Terminate response: {} for user {}'.format(
                     response.text, addr[0]["user"]))


def main(use_pickle):
    # fortiden er termineret i forvejen
    # ad_sync kan ikke lave fremtidige, derfor ingen historik i cache
    mh = MoraHelper(hostname=SETTINGS["mora.base"])
    lc = LoraCache(resolve_dar=False, full_history=False)
    lc.populate_cache(dry_run=use_pickle, skip_associations=True)
    kill_addresses(mh, lc)
    kill_it_connections(mh, lc)


def cli():
    parser = argparse.ArgumentParser(description='Choose backend')
    parser.add_argument('--use-pickle',  action='store_true')
    args = vars(parser.parse_args())
    logger.info('Starting with args: {}'.format(args))
    main(use_pickle=args['use_pickle'])


if __name__ == '__main__':
    cli()
