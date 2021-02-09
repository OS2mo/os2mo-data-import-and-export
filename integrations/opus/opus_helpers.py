import datetime
import hashlib
import json
import logging
import pathlib
import pickle
import sqlite3
import uuid
from operator import itemgetter
from pathlib import Path

import xmltodict
from integrations import cpr_mapper
from integrations.opus import opus_diff_import, opus_import
from exporters.utils.load_settings import load_settings
# from integrations.opus.opus_exceptions import NoNewerDumpAvailable
from integrations.opus.opus_exceptions import (ImporterrunNotCompleted,
                                               RedundantForceException,
                                               RunDBInitException)

SETTINGS = load_settings()
DUMP_PATH = Path(SETTINGS['integrations.opus.import.xml_path'])
START_DATE = datetime.datetime(2019, 1, 1, 0, 0)

logger = logging.getLogger("opusHelper")


def read_cpr_mapping():
    cpr_map = pathlib.Path.cwd() / 'settings' / 'cpr_uuid_map.csv'
    if not cpr_map.is_file():
        logger.error('Did not find cpr mapping')
        raise Exception('Did not find cpr mapping')

    logger.info('Found cpr mapping')
    employee_forced_uuids = cpr_mapper.employee_mapper(str(cpr_map))
    return employee_forced_uuids


def read_available_dumps():
    dumps = {}

    for opus_dump in DUMP_PATH.glob('*.xml'):
        date_part = opus_dump.name[4:18]
        export_time = datetime.datetime.strptime(date_part, '%Y%m%d%H%M%S')
        if export_time > START_DATE:
            dumps[export_time] = opus_dump
    return dumps


def local_db_insert(insert_tuple):
    conn = sqlite3.connect(SETTINGS['integrations.opus.import.run_db'],
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


def initialize_db(run_db):
    logger.info('Force is true, create new db')
    conn = sqlite3.connect(str(run_db))
    c = conn.cursor()
    c.execute("""
    CREATE TABLE runs (id INTEGER PRIMARY KEY,
    dump_date timestamp, status text)
    """)
    conn.commit()
    conn.close()


def next_xml_file(run_db, dumps):
    conn = sqlite3.connect(SETTINGS['integrations.opus.import.run_db'],
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
        # raise NoNewerDumpAvailable('No newer XML dump is available')
        print('No newer dump is available - already done :)')
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




def read_dump_data(dump_file):
    cache_file = pathlib.Path.cwd() / 'tmp' / (dump_file.stem + '.p')
    if not cache_file.is_file():
        data = xmltodict.parse(dump_file.read_text())['kmd']
        with open(str(cache_file), 'wb') as f:
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
    else:
        with open(str(cache_file), 'rb') as f:
            data = pickle.load(f)
    return data


def compare_employees(original, new, force=False):
    # Differences is these keys will not be counted as a difference, unless force
    # is set to true. Notice lastChanged is included here, since we perform a
    # brute-force comparison and does not care for lastChanged.
    skip_keys = ['productionNumber', 'entryIntoGroup', 'invoiceRecipient',
                 '@lastChanged', 'cpr']
    identical = True
    for key in new.keys():
        if key in skip_keys and not force:
            continue
        if not original.get(key) == new[key]:
            identical = False
            msg = 'Changed {} from {} to {}'
            print(msg.format(key, original.get(key), new[key]))
    return identical


def update_employee(employee_number, days):
    from integrations.ad_integration import ad_reader

    employee_mapping = read_cpr_mapping()
    ad_read = ad_reader.ADParameterReader()
    latest_date = None

    current_object = {}
    cut_date = datetime.datetime.now() - datetime.timedelta(days=days)
    dumps = read_available_dumps()

    for date in sorted(dumps.keys()):
        print(date)
        if date < cut_date:
            continue
        dump_file = dumps[date]
        data = read_dump_data(dump_file)

        employees = data['employee']
        for employee in employees:
            if employee['@id'] != employee_number:
                continue

            if employee == current_object:
                continue
            if not compare_employees(current_object, employee):
                if not latest_date:
                    latest_date = date - datetime.timedelta(days=1)
                msg = 'date: {}, lastChanged: {}'
                print(msg.format(date, employee['@lastChanged']))

                diff = opus_diff_import.OpusDiffImport(
                    latest_date,
                    ad_reader=ad_read,
                    employee_mapping=employee_mapping
                )

                if current_object:
                    # If this is not the first edit, we force the lastChanged to that
                    # of the latest known edit.
                    employee['@lastChanged'] = latest_date.strftime('%Y-%m-%d')
                else:
                    employee['@lastChanged'] = employee['entryDate']
                diff.import_single_employment(employee)
                current_object = employee
                latest_date = date


def filter_units(units, filter_ids):
    """
    Filter units such that no unit with a parent-id in filter_ids exist.

    Example:
        >>> units = [(1, None), (2, 1), (3, 1), (4, 2), (5, 2), (6, 3), (7, 5)]
        >>> tup_to_unit = lambda tup: {'@id': tup[0], 'parentOrgUnit': tup[1]}
        >>> units = list(map(tup_to_unit, units))
        >>> get_ids = lambda units: list(map(itemgetter('@id'), units))
        >>> get_ids(filter_units(units, [1]))
        []
        >>> get_ids(filter_units(units, [2]))
        [1, 3, 6]
        >>> get_ids(filter_units(units, [3]))
        [1, 2, 4, 5, 7]
        >>> get_ids(filter_units(units, [3, 5]))
        [1, 2, 4]
        >>> get_ids(filter_units(units, [3, 7]))
        [1, 2, 4, 5]

    Args:
        units: List of units
        filter_ids: List of unit IDs to filter parents on

    Returns:
        list: List of units, with some filtered out
    """

    def get_parent(parent_map, entry):
        """Build a list of parents."""
        parent = parent_map.get(entry, None)
        if parent is None:
            return [entry]
        return [entry] + get_parent(parent_map, parent)

    parent_map = dict(map(itemgetter("@id", "parentOrgUnit"), units))
    filter_set = set(filter_ids)

    def is_disjoint_from_filter_ids(unit):
        """Test for overlap between parents and filter_set."""
        parent_set = set(get_parent(parent_map, unit['@id']))
        return parent_set.isdisjoint(filter_set)

    return list(filter(is_disjoint_from_filter_ids, units))
