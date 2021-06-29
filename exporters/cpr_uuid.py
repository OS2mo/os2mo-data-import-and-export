import os
import time
import json
import pathlib
import logging
from functools import partial

import click
from tqdm import tqdm

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.ad_integration.ad_reader import ADParameterReader


cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())
MORA_BASE = settings['mora.base']

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'cpr_uuid_export.log'

logger = logging.getLogger("cpr_uuid")


# detail_logging = ('AdCommon', 'mora-helper', 'AdReader', 'cpr_uuid')
detail_logging = ('mora-helper', 'AdReader', 'cpr_uuid')
for name in logging.root.manager.loggerDict:
    if name in detail_logging:
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


def create_mapping(helper, use_ad):
    def cache_ad_reader():
        print("Caching all users from AD...")
        t0 = time.time()
        if use_ad:
            ad_reader = ADParameterReader()
            ad_reader.cache_all(print_progress=True)
        logger.info('All users cached, time: {:.0f}s'.format(time.time() - t0))
        print("OK")
        return ad_reader

    def to_user_dict(employee):
        uuid = employee['uuid']
        cpr = employee['cpr_no']

        # AD properties will be enriched if available
        return {
            'cpr': cpr,
            'mo_uuid': uuid,
            'ad_guid': None,
            'sam_account_name': None
        }

    def enrich_user_dict_from_ad(ad_reader, user_dict):
        ad_info = ad_reader.read_user(cpr=user_dict['cpr'], cache_only=True)
        if ad_info:
            user_dict.update({
                'ad_guid': ad_info['ObjectGuid'],
                'sam_account_name': ad_info['SamAccountName']
            })
        return user_dict

    print("Fetching all users from MO...")
    employees = helper.read_all_users()
    total = len(employees)
    print("OK")

    employees = map(to_user_dict, employees)

    if use_ad:
        ad_reader = cache_ad_reader()
        employees = map(partial(enrich_user_dict_from_ad, ad_reader), employees)

    print("Processing all...")
    employees = tqdm(employees, total=total)
    employees = list(employees)
    print("OK")
    return employees


def main(use_ad):
    mh = MoraHelper(hostname=MORA_BASE, export_ansi=True)
    mapping = create_mapping(mh, use_ad)
    fields = ['cpr', 'mo_uuid', 'ad_guid', 'sam_account_name']
    mh._write_csv(fields, mapping, 'cpr_mo_ad_map.csv')


@click.command(help='UUID exporter')
@click.option('--use-ad', is_flag=True)
def cli(**args):
    logger.info('CLI arguments: %r', args)
    main(args['use_ad'])


if __name__ == '__main__':
    cli()
