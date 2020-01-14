import uuid
import json
import pathlib
import hashlib
import logging
import sqlite3
import datetime
from pathlib import Path

from integrations import cpr_mapper
from integrations.opus import opus_import
from integrations.opus import opus_diff_import
from integrations.opus.opus_exceptions import RunDBInitException
from integrations.opus.opus_exceptions import NoNewerDumpAvailable
from integrations.opus.opus_exceptions import RedundantForceException
from integrations.opus.opus_exceptions import ImporterrunNotCompleted

# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

DUMP_PATH = Path(SETTINGS['integrations.opus.import.xml_path'])
START_DATE = datetime.datetime(2019, 1, 1, 0, 0)

logger = logging.getLogger("opusHelper")


def _read_cpr_mapping():
    cpr_map = pathlib.Path.cwd() / 'settings' / 'cpr_uuid_map.csv'
    if not cpr_map.is_file():
        logger.error('Did not find cpr mapping')
        raise Exception('Did not find cpr mapping')

    logger.info('Found cpr mapping')
    employee_forced_uuids = cpr_mapper.employee_mapper(str(cpr_map))
    return employee_forced_uuids


def _read_available_dumps():
    dumps = {}

    for opus_dump in DUMP_PATH.glob('*.xml'):
        date_part = opus_dump.name[4:18]
        export_time = datetime.datetime.strptime(date_part, '%Y%m%d%H%M%S')
        if export_time > START_DATE:
            dumps[export_time] = opus_dump
    return dumps


def _local_db_insert(insert_tuple):
    conn = sqlite3.connect(SETTINGS['integrationso.opus.import.run_db'],
                           detect_types=sqlite3.PARSE_DECLTYPES)
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
    conn = sqlite3.connect(SETTINGS['opus.import.run_db'],
                           detect_types=sqlite3.PARSE_DECLTYPES)
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
    return (next_date, latest_date)


def parse_phone(phone_number):
    validated_phone = None
    if len(phone_number) == 8:
        validated_phone = phone_number
    elif len(phone_number) in (9, 11):
        validated_phone = phone_number.replace(' ', '')
    elif len(phone_number) in (4, 5):
        validated_phone = '0000' + phone_number.replace(' ', '')

    if validated_phone is None:
        logger.warning('Could not parse phone {}'.format(phone_number))
    return validated_phone


def generate_uuid(value):
    """
    Generate a predictable uuid based on org name and a unique value.
    """
    base_hash = hashlib.md5(SETTINGS['municipality.name'].encode())
    base_digest = base_hash.hexdigest()
    base_uuid = uuid.UUID(base_digest)

    combined_value = (str(base_uuid) + str(value)).encode()
    value_hash = hashlib.md5(combined_value)
    value_digest = value_hash.hexdigest()
    value_uuid = uuid.UUID(value_digest)
    return value_uuid


def start_opus_import(importer, ad_reader=None, force=False):
    """
    Start an opus import, run the oldest available dump that
    has not already been imported.
    """
    dumps = _read_available_dumps()

    run_db = Path(SETTINGS['integrations.opus.import.run_db'])
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

    employee_mapping = _read_cpr_mapping()

    opus_importer = opus_import.OpusImport(
        importer,
        org_name=SETTINGS['municipality.name'],
        xml_data=str(xml_file),
        ad_reader=ad_reader,
        import_first=True,
        employee_mapping=employee_mapping
    )
    logger.info('Start import')
    opus_importer.insert_org_units()
    opus_importer.insert_employees()
    opus_importer.add_addresses_to_employees()
    opus_importer.importer.import_all()
    logger.info('Ended import')

    _local_db_insert((xml_date, 'Import ended: {}'))


def start_opus_diff(ad_reader=None):
    """
    Start an opus update, use the oldest available dump that has not
    already been imported.
    """
    dumps = _read_available_dumps()
    run_db = Path(SETTINGS['integrations.opus.import.run_db'])

    employee_mapping = _read_cpr_mapping()

    if not run_db.is_file():
        logger.error('Local base not correctly initialized')
        raise RunDBInitException('Local base not correctly initialized')
    xml_date, latest_date = _next_xml_file(run_db, dumps)
    xml_file = dumps[xml_date]

    _local_db_insert((xml_date, 'Running diff update since {}'))
    msg = 'Start update: File: {}, update since: {}'
    logger.info(msg.format(xml_file, latest_date))
    print(msg.format(xml_file, latest_date))

    diff = opus_diff_import.OpusDiffImport(latest_date, ad_reader=ad_reader,
                                           employee_mapping=employee_mapping)
    diff.start_re_import(xml_file, include_terminations=True)
    logger.info('Ended update')
    _local_db_insert((xml_date, 'Diff update ended: {}'))
