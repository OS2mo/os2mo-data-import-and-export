import json
import pathlib
import argparse

from os2mo_data_import import ImportHelper

from integrations.opus.nectar_import import NectarImport


parser = argparse.ArgumentParser(description='Frederikshavn import')
group = parser.add_mutually_exclusive_group()
group.add_argument('--import', action='store_true', help='New import into empty MO')
args = vars(parser.parse_args())

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

if args['import']:

    importer = ImportHelper(
        create_defaults=True,
        mox_base=SETTINGS['mox.base'],
        mora_base=SETTINGS['mora.base'],
        store_integration_data=False,
        seperate_names=True,
        demand_consistent_uuids=False
    )

    # Where is this unit in the dataset?
    importer.add_organisation_unit(
        identifier='00000350',
        name='Frederikshavn Kommune',
        uuid='aaaaaaaa-bbbbb-bbbb-bbbb-bbbbbbbbbbbb',
        user_key='Frederikshavn Kommune',
        parent_ref=None,
        type_ref='Enhed',
        date_from='1900-01-01',
        date_to=None
    )

    
    nectar = NectarImport(importer)
    nectar.insert_org_units()
    # nectar.create_employees()

    # importer.import_all()

