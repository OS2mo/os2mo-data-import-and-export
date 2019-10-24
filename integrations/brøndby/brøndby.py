import json
import pathlib
import datetime

from os2mo_data_import import ImportHelper
from integrations.SD_Lon.sd_importer import sd_importer


cfg_file = pathlib.Path.cwd() / 'settings' / 'kommune-brøndby.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())

GLOBAL_GET_DATE = datetime.datetime(2019, 9, 15, 0, 0)

importer = ImportHelper(
    create_defaults=True,
    mox_base=settings['mox.base'],
    mora_base=settings['mora.base'],
    store_integration_data=False,
    seperate_names=True
)

sd = sd_importer.SdImport(
    importer,
    settings=settings,
    import_date_from=GLOBAL_GET_DATE,
    ad_info=None,
    manager_rows=None
)

sd.create_ou_tree(create_orphan_container=True)
sd.create_employees()

importer.import_all()
