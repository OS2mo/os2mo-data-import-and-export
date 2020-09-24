import json
import pathlib
import argparse

from os2mo_data_import import ImportHelper

from integrations.ad_integration import ad_reader
from integrations.opus.opus_helpers import update_employee
from integrations.opus.opus_helpers import start_opus_diff
from integrations.opus.opus_helpers import start_opus_import
from integrations.opus.opus_exceptions import RunDBInitException

parser = argparse.ArgumentParser(description='Rebild import')
group = parser.add_mutually_exclusive_group()
group.add_argument('--import', action='store_true', help='New import into empty MO')
group.add_argument('--update', action='store_true', help='Update with next xml file')
group.add_argument('--update-single-user', nargs=1, metavar='Emmploymentnumber',
                   help='Update a single user')
parser.add_argument('--days', nargs=1, type=int, metavar='Days to go back',
                    help='Number of days in the past to sync single user',
                    default=[1000])

args = vars(parser.parse_args())

from integrations.lazy_settings import get_settings
SETTINGS = get_settings()

ad_reader = ad_reader.ADParameterReader()

if args['update']:
    try:
        start_opus_diff(ad_reader=ad_reader)
    except RunDBInitException:
        print('RunDB not initialized')

if args['import']:
    importer = ImportHelper(
        create_defaults=True,
        mox_base=SETTINGS['mox.base'],
        mora_base=SETTINGS['mora.base'],
        store_integration_data=False,
        seperate_names=True,
        demand_consistent_uuids=False
    )

    try:
        start_opus_import(importer, ad_reader=ad_reader, force=True)
    except RunDBInitException:
        print('RunDB not initialized')

if args.get('update_single_user'):
    employment_number = args.get('update_single_user')[0]
    days = args['days'][0]
    update_employee(employment_number, days)
