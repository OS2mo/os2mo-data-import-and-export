import csv
import json
import pathlib

from chardet.universaldetector import UniversalDetector

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
settings = json.loads(cfg_file.read_text())

log_file = pathlib.Path.cwd() / 'mo_integrations_initial_import.log'
log_text = log_file.read_text()

detector = UniversalDetector()
manager_file = settings['integrations.SD_Lon.import.manager_file']
with open(manager_file, 'rb') as csvfile:
    for row in csvfile:
        detector.feed(row)
        if detector.done:
            break
detector.close()
encoding = detector.result['encoding']

department_skip_list = []

manager_rows = []
with open(manager_file, encoding=encoding) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        if row['Leder 1 (cpr-nummer)']:
            new_row = {
                'cpr': row['Leder 1 (cpr-nummer)'].replace('-', ''),
                'ansvar': row['Lederansvar "Leder 1"'],
                'afdeling': row['SD kort navn (afd.kode)']
            }
            manager_rows.append(new_row)

for manager in manager_rows:
    if manager['afdeling'] in department_skip_list:
        continue
    search_string = 'Manager {} to {}'.format(manager['cpr'], manager['afdeling'])
    pos = log_text.find(search_string)
    print(search_string)
    assert (pos > 0)
