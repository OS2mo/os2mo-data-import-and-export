import json
import pathlib
import argparse

from os2mo_data_import import ImportHelper

from integrations.ad_integration import ad_reader
from integrations.opus.opus_helpers import start_opus_diff
from integrations.opus.opus_helpers import start_opus_import
from integrations.opus.opus_exceptions import RunDBInitException

parser = argparse.ArgumentParser(description='Sorø import')
group = parser.add_mutually_exclusive_group()
group.add_argument('--import', action='store_true', help='New import into empty MO')
group.add_argument('--update', action='store_true', help='Update with next xml file')
args = vars(parser.parse_args())

# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

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


    med_name = 'MED Organisation'
    importer.add_klasse(
        identifier=med_name,
        facet_type_ref='org_unit_type',
        user_key=med_name,
        scope='TEXT',
        title=med_name
    )

    importer.add_organisation_unit(
        identifier=med_name,
        name=med_name,
        user_key=med_name,
        type_ref=med_name,
        date_from='1930-01-01',
        date_to=None,
        parent_ref=None
    )

    try:
        start_opus_import(importer, ad_reader=ad_reader, force=True)
    except RunDBInitException:
        print('RunDB not initialized')
