import json
import pathlib

from integrations import cpr_mapper
from os2mo_data_import import ImportHelper
from integrations.SD_Lon import sd_importer
from integrations.ad_integration import ad_reader

# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())

cpr_map = pathlib.Path.cwd() / 'settings' / 'cpr_uuid_map.csv'
if not cpr_map.is_file():
    raise Exception('No mapping file')
employee_mapping = cpr_mapper.employee_mapper(str(cpr_map))

importer = ImportHelper(
    create_defaults=True,
    mox_base=settings['mox.base'],
    mora_base=settings['mora.base'],
    store_integration_data=False,
    seperate_names=True
)

ad_info_reader = ad_reader.ADParameterReader()

sd = sd_importer.SdImport(
    importer,
    ad_info=ad_info_reader,
    employee_mapping=employee_mapping
)

importer.add_klasse(identifier='IT-Org. Alias',
                    uuid='df948904-7ad3-49b0-92a0-e35c50a4bccf',
                    facet_type_ref='org_unit_address_type',
                    user_key='IT-Org. Alias',
                    scope='TEXT',
                    title='IT-Org. Alias')

sd.create_ou_tree(create_orphan_container=True)
sd.create_employees()

importer.import_all()
