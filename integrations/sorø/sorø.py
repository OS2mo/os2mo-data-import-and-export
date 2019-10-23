import os
from os2mo_data_import import ImportHelper

from integrations.ad_integration import ad_reader
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
    demand_consistent_uuids=False
)

ad_reader = ad_reader.ADParameterReader()

med_name = 'MED Organisation'
importer.add_klasse(
    identifier=med_name,
    facet_type_ref='org_unit_type',
    user_key=med_name,
    scope='TEXT',
    title=med_name
)

importer.add_organisation_unit(
    identifier=med_name,
    name=med_name,
    user_key=med_name,
    type_ref=med_name,
    date_from='1900-01-01',
    date_to=None,
    parent_ref=None
)

try:
    start_opus_import(importer, ad_reader=ad_reader, force=True)
except RunDBInitException:
    print('RunDB not initialized')
