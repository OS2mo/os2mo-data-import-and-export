import os
import sys
import datetime

from os2mo_data_import import ImportHelper
import viborg_uuids
sys.path.append('..')
import sd_importer
import ad_reader

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_CODE', 0)
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')

# ORIGIN FOR TESTS WIH ACTUAL API
# GLOBAL_GET_DATE = datetime.datetime(2006, 1, 1, 0, 0) # will not work
# GLOBAL_GET_DATE = datetime.datetime(2009, 1, 1, 0, 0) 
# GLOBAL_GET_DATE = datetime.datetime(2011, 1, 1, 0, 0) 
# GLOBAL_GET_DATE = datetime.datetime(2014, 2, 15, 0, 0)
GLOBAL_GET_DATE = datetime.datetime(2019, 2, 15, 0, 0) # CACHED

importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=True
)

ad_reader = ad_reader.ADParameterReader()

sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    import_date_from=GLOBAL_GET_DATE,
    ad_info=ad_reader
)

sd.create_ou_tree()
sd.create_employees()

importer.import_all()

"""
for info in sd.address_errors.values():
    print(info['DepartmentName'])
    print(info['DepartmentIdentifier'])
    print(info['PostalAddress']['StandardAddressIdentifier'])
    print(info['PostalAddress']['PostalCode'] + ' ' +
          info['PostalAddress']['DistrictName'])
    print()
    print()
print(len(sd.address_errors))
"""
