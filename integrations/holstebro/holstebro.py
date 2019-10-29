import json
import pathlib
import datetime

from integrations import cpr_mapper
from os2mo_data_import import ImportHelper
from integrations.SD_Lon import sd_importer
from integrations.ad_integration import ad_reader

cfg_file = pathlib.Path.cwd() / 'settings' / 'kommune-holstebro.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())

cpr_map = pathlib.Path.cwd() / 'settings' / 'cpr_uuid_map.csv'
if not cpr_map.is_file():
    raise Exception('No mapping file')
employee_mapping = cpr_mapper.employee_mapper(str(cpr_map))

GLOBAL_GET_DATE = datetime.datetime(2019, 10, 15, 0, 0)

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
    settings=settings,
    import_date_from=GLOBAL_GET_DATE,
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
