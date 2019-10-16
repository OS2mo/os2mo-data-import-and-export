import os
import logging
import sqlite3
import datetime
from pathlib import Path

from integrations.opus import opus_import
from integrations.opus.opus_exceptions import RunDBInitException
from integrations.opus.opus_exceptions import NoNewerDumpAvailable
from integrations.opus.opus_exceptions import RedundantForceException
from integrations.opus.opus_exceptions import ImporterrunNotCompleted

RUN_DB = os.environ.get('RUN_DB', None)
MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'Opus Import')

DUMP_PATH = Path('/opt/magenta/dataimport/opus')
START_DATE = datetime.datetime(2019, 8, 2, 0, 0)

# Check this!!!!!!!!!!
# Maybe we should do the logging configuration here!
logger = logging.getLogger("opusImport")


def compare_files():
    import difflib
    dumps = _read_available_dumps()
    names = sorted(dumps.keys())

    # with .open(mode='r') as xmldump:
    #     file0 = xmldump.read()
    # with open(str(dumps[names[1]])) as xmldump:
    #     file1 = xmldump.read()

    # print(file0)

    #    print(dumps[names[0]].read_text().split('\n'))

    diff = difflib.ndiff(
        dumps[names[0]].read_text().split('\n'),
        dumps[names[1]].read_text().split('\n')
    )

    line_number = 0
    for d in diff:
        line_number += 1
        # if d[0] in ('+', '-'):
        if d[0] == '+':
            msg = 'Line: {}: {}'.format(str(line_number).zfill(7), d)
            print(msg)
    # print(list(diff)[0])


def _read_available_dumps():
    dumps = {}

    for opus_dump in DUMP_PATH.glob('*.xml'):
        date_part = opus_dump.name[4:18]
        export_time = datetime.datetime.strptime(date_part, '%Y%m%d%H%M%S')
        if export_time > START_DATE:
            dumps[export_time] = opus_dump
    return dumps


def _local_db_insert(insert_tuple):
    conn = sqlite3.connect(RUN_DB, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    query = 'insert into runs (dump_date, status) values (?, ?)'
    final_tuple = (
        insert_tuple[0],
        insert_tuple[1].format(datetime.datetime.now())
    )
    c.execute(query, final_tuple)
    conn.commit()
    conn.close()


def _initialize_db(run_db):
    logger.info('Force is true, create new db')
    conn = sqlite3.connect(str(run_db))
    c = conn.cursor()
    c.execute("""
    CREATE TABLE runs (id INTEGER PRIMARY KEY,
    dump_date timestamp, status text)
    """)
    conn.commit()
    conn.close()


def _next_xml_file(run_db, dumps):
    conn = sqlite3.connect(RUN_DB, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    query = 'select * from runs order by id desc limit 1'
    c.execute(query)
    row = c.fetchone()
    latest_date = row[1]
    next_date = None
    if 'Running' in row[2]:
        print('Critical error')
        logging.error('Previous run did not return!')
        raise ImporterrunNotCompleted('Previous run did not return!')

    for date in sorted(dumps.keys()):
        if date > latest_date:
            next_date = date
            break
    if next_date is None:
        raise NoNewerDumpAvailable('No newer XML dump is available')
    return next_date


def start_opus_import(importer, ad_reader=None, force=False):
    """
    Start an opus import, run the oldest available dump that
    has not already been imported.
    """
    dumps = _read_available_dumps()

    run_db = Path(RUN_DB)
    if not run_db.is_file():
        logger.error('Local base not correctly initialized')
        if not force:
            raise RunDBInitException('Local base not correctly initialized')
        else:
            _initialize_db(run_db)
        xml_date = sorted(dumps.keys())[0]
    else:
        if force:
            raise RedundantForceException('Used force on existing db')
        xml_date = _next_xml_file(run_db, dumps)

    xml_file = dumps[xml_date]
    _local_db_insert((xml_date, 'Running since {}'))

    opus_importer = opus_import.OpusImport(
        importer,
        org_name=MUNICIPALTY_NAME,
        xml_data=str(xml_file),
        ad_reader=ad_reader,
        import_first=True
    )
    logger.info('Start import')
    opus_importer.insert_org_units()
    opus_importer.insert_employees()
    opus_importer.add_addresses_to_employees()
    opus_importer.importer.import_all()
    logger.info('Ended import')

    _local_db_insert((xml_date, 'Import ended: {}'))


if __name__ == '__main__':
    compare_files()
