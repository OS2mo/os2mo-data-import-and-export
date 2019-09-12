import os
import sys
import datetime

from os2mo_data_import import ImportHelper
sys.path.append('../SD_Lon')
import sd_importer

sys.path.append('../ad_integration')
import ad_reader

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_CODE', 0)
MOX_BASE = os.environ.get('MOX_BASE')
MORA_BASE = os.environ.get('MORA_BASE')

GLOBAL_GET_DATE = datetime.datetime(2019, 9, 1, 0, 0)


importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=False,
    seperate_names=True
)

ad_info_reader = ad_reader.ADParameterReader()

sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    import_date_from=GLOBAL_GET_DATE,
    ad_info=ad_info_reader
)

importer.add_klasse(identifier='IT-Org. Alias',
                    uuid=None,
                    facet_type_ref='org_unit_address_type',
                    user_key='IT-Org. Alias',
                    scope='TEXT',
                    title='IT-Org. Alias')


sd.create_ou_tree(create_orphan_container=True)
sd.create_employees()

importer.import_all()
