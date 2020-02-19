import json
import pathlib
import argparse

from os2mo_data_import import ImportHelper

from integrations.opus.opus_helpers import start_opus_diff
from integrations.opus.opus_helpers import start_opus_import
from integrations.opus.opus_exceptions import RunDBInitException

parser = argparse.ArgumentParser(description='Furesø import')
group = parser.add_mutually_exclusive_group()
group.add_argument('--import', action='store_true', help='New import into empty MO')
group.add_argument('--update', action='store_true', help='Update with next xml file')
args = vars(parser.parse_args())

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())


if args['update']:
    try:
        start_opus_diff(ad_reader=None)
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
        start_opus_import(importer, ad_reader=None, force=True)
    except RunDBInitException:
        print('RunDB not initialized')
