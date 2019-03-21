import os
import sys
from os2mo_data_import import ImportHelper
sys.path.append('..')
import opus_import

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'Opus Import')
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')
XML_FILE_PATH = os.environ.get('XML_FILE_PATH', '')

importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='Opus-Import',
    end_marker='OPUS_STOP!',
    store_integration_data=True
)

# importer.new_itsystem(
#     identifier='AD',
#     system_name='Active Directory'
# )

opus = opus_import.OpusImport(
    importer,
    MUNICIPALTY_NAME,
    XML_FILE_PATH
)

opus.insert_org_units()
opus.insert_employees()
opus.add_addresses_to_employees()

importer.import_all()
