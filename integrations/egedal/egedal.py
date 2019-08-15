import os
import sys
import datetime

from os2mo_data_import import ImportHelper
sys.path.append('../SD_Lon')
import sd_importer

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_CODE', 0)
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')

GLOBAL_GET_DATE = datetime.datetime(2019, 6, 13, 0, 0)

importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=True
)

sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    import_date_from=GLOBAL_GET_DATE,
    org_only=True,
    org_id_prefix='test'
)

sd.create_ou_tree(
    create_orphan_container=False,
    # Main tree encoded with prefix 'test' 
    sub_tree='6d8f88af-b93a-7455-9b4a-970e2dafbf7c' 
)

# sd.create_ou_tree(
#   create_orphan_container=True,
#    sub_tree=None
#)

importer.import_all()

