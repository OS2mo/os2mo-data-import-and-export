import os
from os2mo_data_import import ImportHelper

from integrations.opus.opus_helpers import start_opus_import
from integrations.opus.opus_exceptions import RunDBInitException


MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')


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

try:
    start_opus_import(importer, ad_reader=None, force=True)
except RunDBInitException:
    print('RunDB not initialized')
