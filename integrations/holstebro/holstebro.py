import json
import pathlib
import datetime

from os2mo_data_import import ImportHelper
from integrations.SD_Lon import sd_importer
from integrations.ad_integrations import ad_reader

cfg_file = pathlib.Path.cwd() / 'settings' / 'kommune-holstebro.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())

GLOBAL_GET_DATE = datetime.datetime(2019, 10, 1, 0, 0)

importer = ImportHelper(
    create_defaults=True,
    mox_base=settings['mox.base'],
    mora_base=settings['mora.base'],
    # system_name='SD-Import',
    # end_marker='SDSTOP',
    store_integration_data=False,
    seperate_names=True
)

ad_info_reader = ad_reader.ADParameterReader()

sd = sd_importer.SdImport(
    importer,
    settings=settings,
    import_date_from=GLOBAL_GET_DATE,
    ad_info=ad_info_reader
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
