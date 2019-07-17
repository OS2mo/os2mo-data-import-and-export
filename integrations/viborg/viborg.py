import os
import csv
import sys
import datetime
from chardet.universaldetector import UniversalDetector

from os2mo_data_import import ImportHelper
sys.path.append('../SD_Lon')
import sd_importer

sys.path.append('../ad_integration')
import ad_reader

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_CODE', 0)
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')
MANAGER_FILE = os.environ.get('MANAGER_FILE')

# ORIGIN FOR TESTS WIH ACTUAL API
# GLOBAL_GET_DATE = datetime.datetime(2006, 1, 1, 0, 0) # will not work
# GLOBAL_GET_DATE = datetime.datetime(2009, 1, 1, 0, 0)
GLOBAL_GET_DATE = datetime.datetime(2019, 7, 15, 0, 0)


importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=True,
    seperate_names=True
)

ad_reader = ad_reader.ADParameterReader()

detector = UniversalDetector()
with open(MANAGER_FILE, 'rb') as csvfile:
    for row in csvfile:
        detector.feed(row)
        if detector.done:
            break
detector.close()
encoding = detector.result['encoding']

manager_rows = []
with open(MANAGER_FILE, encoding=encoding) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        if row['Leder 1 (cpr-nummer)']:
            new_row = {
                'cpr': row['Leder 1 (cpr-nummer)'].replace('-', ''),
                'ansvar': row['Lederansvar "Leder 1"'],
                'afdeling': row['SD kort navn (afd.kode)']
            }
            manager_rows.append(new_row)

        if row['Leder 2 (cpr-nummer)'].strip():
            new_row = {
                'cpr': row['Leder 2 (cpr-nummer)'].replace('-', ''),
                'ansvar': row['Lederansvar "Leder 2"'],
                'afdeling': row['SD kort navn (afd.kode)']
            }
            manager_rows.append(new_row)


sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    import_date_from=GLOBAL_GET_DATE,
    ad_info=ad_reader,
    manager_rows=manager_rows
)

sd.create_ou_tree(create_orphan_container=False)
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
