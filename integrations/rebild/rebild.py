import os
import pathlib
from integrations import cpr_mapper
from os2mo_data_import import ImportHelper

from integrations.ad_integration import ad_reader
from integrations.opus.opus_helpers import start_opus_import
from integrations.opus.opus_exceptions import RunDBInitException


MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')

cpr_map = pathlib.Path.cwd() / 'integrations' / 'rebild' / 'cpr_uuid_map.csv'
if not cpr_map.is_file():
    raise Exception('No mapping file')
employee_mapping = cpr_mapper.employee_mapper(str(cpr_map))

importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='Opus-Import',
    end_marker='OPUS_STOP!',
    store_integration_data=True,
    seperate_names=True,
    demand_consistent_uuids=True
)

ad_reader = ad_reader.ADParameterReader()

try:
    start_opus_import(importer, ad_reader=ad_reader, force=True,
                      employee_mapping=employee_mapping)
except RunDBInitException:
    print('RunDB not initialized')
