import os
import csv
import json
import pathlib
import datetime
from chardet.universaldetector import UniversalDetector

from os2mo_data_import import ImportHelper
from integrations.SD_Lon.sd_importer import  sd_importer
from integrations.ad_integration import ad_reader

cfg_file = pathlib.Path.cwd() / 'settings' / 'kommune-viborg.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())

MANAGER_FILE = os.environ.get('MANAGER_FILE')
GLOBAL_GET_DATE = datetime.datetime(2019, 9, 15, 0, 0)

importer = ImportHelper(
    create_defaults=True,
    mox_base=settings['mox.base'],
    mora_base=settings['mora.base'],
    # system_name='SD-Import',
    # end_marker='SDSTOP',
    store_integration_data=False,
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
    settings=settings,
    import_date_from=GLOBAL_GET_DATE,
    ad_info=ad_reader,
    manager_rows=manager_rows
)

importer.add_klasse(identifier='IT-Org. Alias',
                    uuid='aa4b3520-4ee9-4ac2-9380-a0da852bb538',
                    facet_type_ref='org_unit_address_type',
                    user_key='IT-Org. Alias',
                    scope='TEXT',
                    title='IT-Org. Alias')

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
