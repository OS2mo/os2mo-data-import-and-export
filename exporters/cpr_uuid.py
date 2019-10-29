import os
import time
import json
import pathlib
import logging
import argparse

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
    t0 = time.time()
    org = helper.read_organisation()

    if use_ad:
        ad_reader = ADParameterReader()
        ad_reader.cache_all()
    logger.info('All users cached, time: {:.0f}s'.format(time.time() - t0))

    mapping = []

    i = 0
    employees = helper.read_all_users()

    # Restart timing for more accurate estimation of remaining
    t0 = time.time()

    for i in range(0, len(employees)):
        if i % 50 == 1:
            delta_t = time.time() - t0
            estimated_time = delta_t * len(employees) / i
            time_left = estimated_time - delta_t
            msg = '{}/{}, expected total: {:.0f}s, left: {:.0f}s'
            logger.debug(msg.format(i, len(employees), estimated_time, time_left))
            print(msg.format(i, len(employees), estimated_time, time_left))

        uuid = employees[i]['uuid']
        mo_user = helper.read_user(uuid, org_uuid=org)
        cpr = mo_user['cpr_no']

        user = {  # AD properties will be overwritten if available
            'cpr': cpr,
            'mo_uuid': uuid,
            'ad_guid': None,
            'sam_account_name': None
        }

        if use_ad:
            ad_info = ad_reader.read_user(cpr=cpr, cache_only=True)
            if ad_info:
                user['ad_guid'] = ad_info['ObjectGuid']
                user['sam_account_name'] = ad_info['SamAccountName']

        mapping.append(user)

    return mapping


def main(use_ad):
    mh = MoraHelper(hostname=MORA_BASE, export_ansi=True)
    mapping = create_mapping(mh, use_ad)
    fields = ['cpr', 'mo_uuid', 'ad_guid', 'sam_account_name']
    mh._write_csv(fields, mapping, 'cpr_mo_ad_map.csv')


def cli():
    """
    Command line interface for the AD writer class.
    """
    parser = argparse.ArgumentParser(description='UUID exporter')
    parser.add_argument('--use-ad', action='store_true')
    args = vars(parser.parse_args())
    logger.info('CLI arguments: {}'.format(args))

    use_ad = args.get('use_ad')
    main(use_ad)


if __name__ == '__main__':
    cli()
