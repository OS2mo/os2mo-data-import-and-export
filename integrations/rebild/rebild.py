import os
import sys
from os2mo_data_import import ImportHelper
sys.path.append('../opus')
import opus_helpers
from opus_exceptions import RunDBInitException

sys.path.append('../ad_integration')
import ad_reader


MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')


importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='Opus-Import',
    end_marker='OPUS_STOP!',
    store_integration_data=True,
    seperate_names=True
)

ad_reader = ad_reader.ADParameterReader()

try:
    opus_helpers.start_opus_import(importer, ad_reader=ad_reader, force=True)
except RunDBInitException:
    print('RunDB not initialized')
    exit()

# opus = opus_import.OpusImport(
#     importer,
#     MUNICIPALTY_NAME,
#     XML_FILE_PATH,
#     ad_reader=ad_reader,
#     import_first=True
# )

# opus.insert_org_units()
# opus.insert_employees()
# opus.add_addresses_to_employees()

# importer.import_all()
