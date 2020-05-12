import csv
import json
import logging
import pathlib


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'export_compare.log'

logger = logging.getLogger('mo_lora_compare')

for name in logging.root.manager.loggerDict:
    if name in ('mo_lora_compare'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())


def load_csv(file_name):
    logger.info('Load {}'.format(file_name))
    rows = []
    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            rows.append(row)
    return rows


def compare_exports(lc_rows, mo_rows, id_key=None):
    not_found = []

    for row in lc_rows:
        if row in mo_rows:
            mo_rows.remove(row)
        else:
            not_found.append(row)

    if id_key:
        for mo_row in mo_rows:
            for lc_row in lc_rows:
                if mo_row[id_key] == lc_row[id_key]:
                    mo_set = set(mo_row.items())
                    lc_set = set(lc_row.items())
                    msg = 'Diff in {}: {}'
                    print(msg.format(lc_row[id_key], mo_set ^ lc_set))
                    logger.info(msg.format(lc_row[id_key], mo_set ^ lc_set))

    if mo_rows or not_found:
        logger.info('lc rows not found in mo_rows: {}'.format(not_found))
        logger.info('Remaining mo_rows: {}'.format(mo_rows))
        logger.info('MO rows: {}'.format(len(mo_rows)))
        logger.info('lc rows: {}'.format(len(not_found)))
        print('MO rows: {}'.format(len(mo_rows)))
        print('lc rows: {}'.format(len(not_found)))
    else:
        print('The files are identical')
    print()

if __name__ == '__main__':
    dest_folder = pathlib.Path(SETTINGS['mora.folder.query_export'])

    lora_root = dest_folder
    mo_root = dest_folder / 'mo_generated'

    tests = [
        ('plan2learn_organisation.csv', 'AfdelingsID', None),
        ('plan2learn_engagement.csv', 'EngagementId', None),
        ('plan2learn_stillingskode.csv', 'StillingskodeID', None),
        ('plan2learn_bruger.csv', 'BrugerId', None),
        ('viborg_externe.csv', 'Tjenestenummer', None)
    ]

    for test in tests:
        print('Testing {}'.format(test[0]))
        lc_rows = load_csv(str(lora_root / test[0]))
        mo_rows = load_csv(str(mo_root / test[0]))
        compare_exports(lc_rows, mo_rows, test[1])

    # Todo: Turn this into a combination of nicely formated output +
    # more comprehensive log-file

    # lc_rows = load_csv(str(dest_folder / 'plan2learn_leder.csv'))
    # mo_rows = load_csv(str(dest_folder / 'prod' / 'plan2learn_leder.csv'))
    # Todo: Here we need a combination of two keys to uniquely identify a row
    # compare_exports(lc_rows, mo_rows)
