import csv
import functools
import hashlib
import logging
import pathlib
import requests

from uuid import UUID


import payloads  # File in same folder

from datetime import datetime
from chardet.universaldetector import UniversalDetector

from os2mo_data_import import ImportHelper
from os2mo_helpers.mora_helpers import MoraHelper

LOG_LEVEL = logging.INFO
LOG_FILE = 'mo_initial_import.log'

logger = logging.getLogger('århusImport')

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)

log_format = logging.Formatter(
    "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
)
stdout_log_handler = logging.StreamHandler()
stdout_log_handler.setFormatter(log_format)
stdout_log_handler.setLevel(logging.DEBUG)  # this can be higher
logging.getLogger().addHandler(stdout_log_handler)

for name in logging.root.manager.loggerDict:
    if name in ('moImporterUtilities'):
        logging.getLogger(name).setLevel(logging.WARNING)

UNIT_DAR = ('Postadresse', 'org_unit_address_type', 'DAR', 'AddressMailUnit', '00000000-0000-0000-0000-000000000000')
UNIT_EAN = ( 'EAN nummer', 'org_unit_address_type', 'EAN', 'EANUnit', "500b06b0-39bd-4a50-962c-bc6f4ac63411")
UNIT_EMAIL = ( 'Email', 'org_unit_address_type', 'EMAIL', 'EmailUnit', "03300c9a-bd3f-4fad-b3fb-712c3b902d65")
UNIT_FAX = ('Fax', 'org_unit_address_type', 'PHONE', 'FaxUnit', "d8563d4e-fabf-4704-becf-87292ce889b8")
UNIT_PHONE = ( 'Telefon', 'org_unit_address_type', 'PHONE', 'PhoneUnit', "adb476ad-0860-4ec0-9df3-95f9fd4915b5")
UNIT_WEB = ( 'Webadresse', 'org_unit_address_type', 'WWW', 'WebUnit', "74f658d4-094d-4b11-9d2a-d39d1031877b")
PERSON_EMAIL = ( 'Email', 'employee_address_type', 'EMAIL', 'EmailEmployee', '00000000-0000-0000-0000-000000000001')
PERSON_WORK_PHONE = ( 'Arbejdstelefon', 'employee_address_type', 'PHONE', 'WorkPhoneEmployee', "7169f171-f76c-4c9a-bee0-5e39e5c27eb2")
PERSON_MOBILE_PHONE = ( 'Mobiltelefon', 'employee_address_type', 'PHONE', 'MobilePhoneEmployee', "225f5d0b-75f5-4f64-8f43-aede29f958a8")

UNKNOWN_JOB_FUNCTION = ('Ukendt stillingsbetegnelse', 'engagement_job_function', None, None, "80f1d01e-60e1-4eca-9b36-932412de5a3b")

ADDRESS_TUPLES = [
    ('Export_PDB_orgenhed_ADR.csv', 'ADRUUID', 'OrgenhedUUID', UNIT_DAR),
    ('Export_PDB_orgenhed_ean.csv', 'ean', 'OrgenhedUUID', UNIT_EAN),
    ('Export_PDB_orgenhed_email.csv', 'email', 'OrgenhedUUID', UNIT_EMAIL),
    ('Export_PDB_orgenhed_fax.csv', 'fax', 'OrgenhedUUID', UNIT_FAX),
    ('Export_PDB_orgenhed_tlf.csv', 'tlf', 'OrgenhedUUID', UNIT_PHONE),
    ('Export_PDB_orgenhed_webadresse.csv', 'webadresse', 'OrgenhedUUID',
    UNIT_WEB),
    ('Export_PDB_person_email.csv', 'Email', 'PersonUUID', PERSON_EMAIL),
    ('Export_PDB_person_tlf1.csv', 'Telefon', 'PersonUUID', PERSON_WORK_PHONE),
    ('Export_PDB_person_tlf2.csv', 'Telefon', 'PersonUUID',
    PERSON_MOBILE_PHONE),
]

# General file cache
CACHE = {}

# Cache for mapping between job_function name and UUID
JOB_FUNCTION_CACHE = {}


class ÅrhusImport(object):
    def __init__(self):
        self.csv_path = pathlib.Path('/home/cm/tmp/aak')

        self.importer = ImportHelper(
            create_defaults=True,
            mox_base='http://localhost:8080',
            mora_base='http://localhost:5000',
            store_integration_data=False,
            seperate_names=True
        )
        self.importer.add_organisation(
            identifier='Århus Kommune',
            user_key='Århus Kommune',
            municipality_code=751,
            uuid="b50d4f00-bd65-4836-937c-9b27fe6f71f2"
        )

    def _read_csv(self, filename):
        file_path = self.csv_path / filename
        rows = []

        detector = UniversalDetector()
        with open(str(file_path), 'rb') as csvfile:
            for row in csvfile:
                detector.feed(row)
                if detector.done:
                    break
        detector.close()
        encoding = detector.result['encoding']

        with open(str(file_path), encoding=encoding) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                rows.append(row)
        return rows

    def _generate_uuid(self, *values):
        """
        Generates an UUID based on a list of values
        """

        def hash_fn(x1, x2):
            combined_value = (str(x1) + str(x2)).encode()
            value_hash = hashlib.md5(combined_value)
            value_digest = value_hash.hexdigest()
            return str(UUID(value_digest))

        return functools.reduce(hash_fn, values)

    def _import_klasse(self, titel, facet, scope='TEXT', bvn=None, uuid=None):
        if bvn is None:
            bvn = titel
        self.importer.add_klasse(
            identifier=titel + facet,
            uuid=uuid,
            facet_type_ref=facet,
            user_key=bvn,
            scope=scope,
            title=titel
        )

    def _import_engagement_types(self):
        filename = 'Export_STAM_UUID_Engagementstype.csv'
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)
        logger.info("Importing {} engagement types".format(len(rows)))
        for row in rows:
            logger.debug(row['Engagementstype'])
            self._import_klasse(row['Engagementstype'], 'engagement_type',
                                uuid=row['EngagementstypeUUID'])

    def _import_unit_types(self):
        filename = 'Export_STAM_UUID_Enhedstype.csv'
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)
        logger.info("Importing {} unit types".format(len(rows)))
        for row in rows:
            logger.debug(row['Enhedstype'])
            self._import_klasse(row['Enhedstype'], 'org_unit_type',
                                uuid=row['EnhedstypeUUID'])

    def _import_job_functions(self):
        filename = 'Export_STAM_UUID_Stillingsbetegnelse.csv'
        rows = self._read_csv(filename)
        logger.info("Parsing {}".format(filename))
        logger.info("Importing {} job functions".format(len(rows)))
        for row in rows:
            job_function = row['Stillingsbetegnelse']
            logger.debug(job_function)
            facet = 'engagement_job_function'
            self._import_klasse(job_function,
                                facet,
                                uuid=row['StillingBetUUID'],
                                )
            JOB_FUNCTION_CACHE[job_function] = row['StillingBetUUID']

    def _import_remaining_classes(self):
        logger.info("Importing remaining classes")

        for clazz in [
            UNIT_DAR, UNIT_EAN, UNIT_EMAIL, UNIT_FAX, UNIT_PHONE, UNIT_WEB,
            PERSON_EMAIL, PERSON_WORK_PHONE, PERSON_MOBILE_PHONE, UNKNOWN_JOB_FUNCTION
        ]:
            titel, facet, scope, bvn, uuid = clazz
            self._import_klasse(
                titel=titel,
                facet=facet,
                scope=scope,
                bvn=bvn,
                uuid=uuid
            )

    def _import_users(self):
        filename = 'Export_PDB_person.csv'
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)
        logger.info('Importing {} users'.format(len(rows)))
        for row in rows:
            logger.debug('{} {}'.format(row['Fornavn'], row['Efternavn']))
            self.importer.add_employee(
                name=(row['Fornavn'], row['Efternavn']),
                identifier=row['PersonUUID'],
                cpr_no=row['CPR'],
                user_key=row['CPR'],
                uuid=row['PersonUUID']
            )

    def _parse_file(self, filename, uuid_fn):
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)

        required_fields = ['PersonUUID', 'OrgenhedUUID']

        object_history = {}
        for row in rows:
            uuid = uuid_fn(row)

            invalid = False
            for field in required_fields:
                if field in row and row[field] == 'NULL':
                    logger.warning("{} is missing: {}".format(field, row))
                    invalid = True
            if invalid:
                continue

            if row['Startdato'] == 'NULL':
                logger.warning("Missing 'from_date', skipping: {}".format(row))
                continue
            else:
                from_date = datetime.strptime(row['Startdato'], '%d-%m-%Y')

            if row['Slutdato'] == '31-12-9999':
                end_date = None
            else:
                end_date = datetime.strptime(row['Slutdato'], '%d-%m-%Y')

            if end_date and from_date and end_date < from_date:
                logger.warning("End date before start date: {}".format(row))
                continue

            row['from_date'] = from_date
            row['end_date'] = end_date
            row['from_date_mo'] = datetime.strftime(from_date, "%Y-%m-%d")
            if end_date is None:
                row['end_date_mo'] = None
            else:
                row['end_date_mo'] = datetime.strftime(end_date, "%Y-%m-%d")

            unit_history = object_history.setdefault(uuid, [])
            unit_history.append(row)

        for org_uuid, history_list in object_history.items():
            object_history[org_uuid] = sorted(history_list, key=lambda x: x['from_date'])

        CACHE[filename] = object_history

        return object_history

    def _parse_file_with_uuid(self, filename, uuid_field):
        """
        Parse a file with pre-determined UUIDs
        :param filename: Name of file
        :param uuid_field: Name of field containing UUID
        """
        def uuid_fn(row):
            return row[uuid_field]
        return self._parse_file(filename, uuid_fn)

    def _initial_import_units(self):
        unit_info = self._parse_file_with_uuid('Export_PDB_orgenhed.csv', 'OrgenhedUUID')
        logger.info("Importing {} units".format(len(unit_info)))
        for uuid, unit in unit_info.items():
            parent = unit[0]['ParentOrgenhedUUID']
            if parent not in unit_info:
                print('Parent not found')
                parent = None

            logger.debug(unit[0]['Enhedsnavn'])
            self.importer.add_organisation_unit(
                identifier=uuid,
                uuid=uuid,
                name=unit[0]['Enhedsnavn'],
                user_key=unit[0]['OrgenhedID'],
                type_ref=unit[0]['Enhedstype'] + 'org_unit_type',
                date_from=unit[0]['from_date_mo'],
                date_to=unit[0]['end_date_mo'],
                parent_ref=parent
            )

    def _initial_import_addresses(self):
        logger.info("Importing addresses")

        for address_info in ADDRESS_TUPLES:
            filename, fieldname, relname, address_type = address_info

            # This is the same format we use when creating the classes earlier
            address_title, address_facet, *_ = address_type
            type_ref = address_title + address_facet

            def uuid_fn(row):
                return self._generate_uuid(row[relname], fieldname)

            address_info = self._parse_file(filename, uuid_fn)
            logger.info('Importing {} addresses'.format(len(address_info)))
            for uuid, address in address_info.items():
                logger.debug('Importing {} to {}'.format(address[0][fieldname], address[0][relname]))
                try:
                    self.importer.add_address_type(
                        uuid=uuid,
                        employee=address[0][relname] if relname == 'PersonUUID' else None,
                        organisation_unit=address[0][relname] if relname == 'OrgenhedUUID' else None,
                        type_ref=type_ref,
                        value=address[0][fieldname],
                        date_from=address[0]['from_date_mo'],
                        date_to=address[0]['end_date_mo']
                    )
                except ReferenceError as e:
                    logger.warning("{}: {}".format(str(e), address))

    def _initial_import_engagements(self):

        def uuid_fn(row):
            return self._generate_uuid(row['PersonUUID'], row['OrgenhedUUID'], row['Stillingsbetegnelse'])

        def get_job_function_ref(engagement):
            job_function = engagement['Stillingsbetegnelse']
            if job_function == 'NULL':
                job_function = UNKNOWN_JOB_FUNCTION[0]
            elif job_function not in JOB_FUNCTION_CACHE:
                logger.warning("Job function {} is not known.".format(job_function))
                job_function = UNKNOWN_JOB_FUNCTION[0]
            return job_function + 'engagement_job_function'

        engagement_info = self._parse_file('Export_PDB_person_rel.csv', uuid_fn)

        logger.info("Importing {} engagements".format(len(engagement_info)))
        for uuid, engagements in engagement_info.items():
            engagement = engagements[0]
            engagement_type_ref = engagement['Engagementstype'] + "engagement_type"
            try:
                self.importer.add_engagement(
                    employee=engagement["PersonUUID"],
                    organisation_unit=engagement["OrgenhedUUID"],
                    identifier=uuid,
                    uuid=uuid,
                    job_function_ref=(get_job_function_ref(engagement)),
                    engagement_type_ref=engagement_type_ref,
                    date_from=engagement['from_date_mo'],
                    date_to=engagement['end_date_mo'],
                )
            except KeyError as e:
                logger.warning("{}: {}".format(str(e), engagement))

    def _diff_import_units(self):
        logger.info('Importing historic org unit data')

        unit_info = CACHE['Export_PDB_orgenhed.csv']
        for uuid, unit_row in unit_info.items():
            if len(unit_row) == 1:
                continue
            for unit in unit_row[1:]:
                parent = unit['ParentOrgenhedUUID']
                if parent not in unit_info:
                    print('Parent not found')
                    parent = None
                payload = payloads.edit_org_unit(
                    user_key=unit['OrgenhedID'],
                    name=unit['Enhedsnavn'],
                    unit_uuid=uuid,
                    parent=parent,
                    ou_type=unit['Enhedstype']+'org_unit_type',
                    from_date=unit['from_date_mo'],
                    to_date=unit['end_date_mo']
                )
                response = self.helper._mo_post('details/edit', payload)
                response.raise_for_status()

    def _diff_import_addresses(self):
        logger.info('Importing historic address data')
        for address_info in ADDRESS_TUPLES:
            filename, fieldname, relname, address_type = address_info
            address_title, address_facet, _, _, address_type_uuid = address_type
            address_objects = CACHE[filename]

            for uuid, addresses in address_objects.items():
                if len(addresses) == 1:
                    # We've already imported the first row
                    continue
                for address in addresses[1:]:
                    address_args = {
                        'address_type': {'uuid': address_type_uuid},
                        'value': address[fieldname],
                        'validity': {
                            'from': address['from_date_mo'],
                            'to': address['end_date_mo']
                        }
                    }

                    if relname == "PersonUUID":
                        address_args['person'] = {'uuid': address[relname]}
                    elif relname == "OrgenhedUUID":
                        address_args['org_unit'] = {'uuid': address[relname]}

                    payload = payloads.edit_address(address_args, uuid)
                    response = self.helper._mo_post('details/edit', payload)
                    response.raise_for_status()

    def _diff_import_engagements(self):
        logger.info('Importing historic engagement data')

        engagement_info = CACHE['Export_PDB_person_rel.csv']

        def get_job_function_uuid(engagement):
            job_function = engagement['Stillingsbetegnelse']
            if job_function == 'NULL':
                return UNKNOWN_JOB_FUNCTION[4]
            return JOB_FUNCTION_CACHE[job_function]

        for uuid, engagements in engagement_info.items():
            if len(engagements) == 1:
                continue
            for engagement in engagements[1:]:
                payload = payloads.edit_engagement(
                    engagement_uuid=uuid,
                    job_function_uuid=get_job_function_uuid(engagement),
                    org_unid_uuid=engagement['OrgenhedUUID'],
                    from_date=engagement['from_date_mo'],
                    to_date=engagement['end_date_mo']
                )
                response = self.helper._mo_post('details/edit', payload)
                response.raise_for_status()

    def initial_import(self):
        self._import_engagement_types()
        self._import_unit_types()
        self._import_job_functions()
        self._import_remaining_classes()
        self._import_users()
        self._initial_import_units()
        self._initial_import_addresses()
        self._initial_import_engagements()

        self.importer.import_all()

    def diff_import(self):
        self.helper = MoraHelper(hostname='http://localhost:5000',
                                 use_cache=False)
        try:
            _ = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        self._diff_import_units()
        self._diff_import_addresses()
        self._diff_import_engagements()


if __name__ == '__main__':
    r = requests.get('http://localhost:8080/db/truncate')
    r.raise_for_status()
    print(r.text)

    århus = ÅrhusImport()
    århus.initial_import()
    århus.diff_import()
