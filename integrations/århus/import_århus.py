import asyncio
import csv
import functools
import hashlib
import logging
import os
import pathlib
import sys
from datetime import datetime, timedelta
from uuid import UUID

import requests
from aiohttp import ClientSession, TCPConnector, ClientResponseError
from chardet.universaldetector import UniversalDetector

import payloads  # File in same folder
from os2mo_data_import import ImportHelper
from os2mo_data_import.mox_data_types import Itsystem
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
UNIT_CVR = ( 'CVR nummer', 'org_unit_address_type', 'TEXT', 'CVRUnit', "214fff73-0da6-48dc-ad8f-1e380fc3277c")
UNIT_PNUMBER = ( 'P-nummer', 'org_unit_address_type', 'PNUMBER', 'PNumber', "8363e139-7ac1-4ab8-85cb-913ad443de33")
UNIT_EMAIL = ( 'Email', 'org_unit_address_type', 'EMAIL', 'EmailUnit', "03300c9a-bd3f-4fad-b3fb-712c3b902d65")
UNIT_FAX = ('Fax', 'org_unit_address_type', 'PHONE', 'FaxUnit', "d8563d4e-fabf-4704-becf-87292ce889b8")
UNIT_PHONE = ( 'Telefon', 'org_unit_address_type', 'PHONE', 'PhoneUnit', "adb476ad-0860-4ec0-9df3-95f9fd4915b5")
UNIT_WEB = ( 'Webadresse', 'org_unit_address_type', 'WWW', 'WebUnit', "74f658d4-094d-4b11-9d2a-d39d1031877b")
PERSON_EMAIL = ( 'Email', 'employee_address_type', 'EMAIL', 'EmailEmployee', '00000000-0000-0000-0000-000000000001')
PERSON_WORK_PHONE = ( 'Arbejdstelefon', 'employee_address_type', 'PHONE', 'WorkPhoneEmployee', "7169f171-f76c-4c9a-bee0-5e39e5c27eb2")
PERSON_MOBILE_PHONE = ( 'Mobiltelefon', 'employee_address_type', 'PHONE', 'MobilePhoneEmployee', "225f5d0b-75f5-4f64-8f43-aede29f958a8")

UNKNOWN_JOB_FUNCTION = ('Ukendt stillingsbetegnelse', 'engagement_job_function', None, None, "80f1d01e-60e1-4eca-9b36-932412de5a3b")
UNKNOWN_ORG_UNIT_TYPE = ('Ukendt enhedstype', 'org_unit_type', None, None, "01aadec6-ec7e-4cc8-af76-1f582adda8f7")

ADDRESS_TUPLES = [
    ('Export_PDB_orgenhed_ADR.csv', 'ADRUUID', 'OrgenhedUUID', UNIT_DAR),
    ('Export_PDB_orgenhed_ean.csv', 'ean', 'OrgenhedUUID', UNIT_EAN),
    ('Export_PDB_orgenhed_email.csv', 'email', 'OrgenhedUUID', UNIT_EMAIL),
    ('Export_PDB_orgenhed_fax.csv', 'fax', 'OrgenhedUUID', UNIT_FAX),
    ('Export_PDB_orgenhed_tlf.csv', 'tlf', 'OrgenhedUUID', UNIT_PHONE),
    ('Export_PDB_orgenhed_webadresse.csv', 'webadresse', 'OrgenhedUUID', UNIT_WEB),
    ('Export_PDB_person_email.csv', 'Email', 'PersonUUID', PERSON_EMAIL),
    ('Export_PDB_person_tlf1.csv', 'Telefon', 'PersonUUID', PERSON_WORK_PHONE),
    ('Export_PDB_person_tlf2.csv', 'Telefon', 'PersonUUID', PERSON_MOBILE_PHONE),
]


# Caches for mapping between names and UUIDs
# The cache is lower-cased to account for inconsistencies
ORG_UNIT_TYPE_CACHE = {}


class ÅrhusImport(object):
    def __init__(self):
        csv_path = os.environ['AARHUS_CSV_PATH']

        self.csv_path = pathlib.Path(csv_path)

        self.importer = ImportHelper(
            create_defaults=True,
            mox_base='http://localhost:8080',
            mora_base='http://localhost:5002',
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
            engagementstype = row['Engagementstype']
            uuid = row['EngagementstypeUUID']
            self._import_klasse(engagementstype, 'engagement_type',
                                uuid=uuid)

    def _import_unit_types(self):
        filename = 'Export_STAM_UUID_Enhedstype.csv'
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)
        logger.info("Importing {} unit types".format(len(rows)))
        for row in rows:
            enhedstype = row['Enhedstype']
            uuid = row['EnhedstypeUUID']
            self._import_klasse(enhedstype, 'org_unit_type',
                                uuid=uuid)

            ORG_UNIT_TYPE_CACHE[enhedstype.lower()] = uuid

        titel, facet, scope, bvn, uuid = UNKNOWN_ORG_UNIT_TYPE
        self._import_klasse(
            titel=titel,
            facet=facet,
            uuid=uuid
        )

    def _import_job_functions(self):
        filename = 'Export_STAM_UUID_Stillingsbetegnelse.csv'
        rows = self._read_csv(filename)
        logger.info("Parsing {}".format(filename))
        logger.info("Importing {} job functions".format(len(rows)))
        for row in rows:
            job_function = row['Stillingsbetegnelse']
            logger.debug(job_function)
            facet = 'engagement_job_function'
            uuid = row['StillingBetUUID']
            self._import_klasse(job_function,
                                facet,
                                uuid=uuid,
                                )

        titel, facet, scope, bvn, uuid = UNKNOWN_JOB_FUNCTION
        self._import_klasse(
            titel=titel,
            facet=facet,
            uuid=uuid
        )

    def _import_remaining_classes(self):
        logger.info("Importing remaining classes")

        for clazz in [
            UNIT_DAR, UNIT_EAN, UNIT_EMAIL, UNIT_FAX, UNIT_PHONE, UNIT_WEB,
            PERSON_EMAIL, PERSON_WORK_PHONE, PERSON_MOBILE_PHONE, UNIT_CVR, UNIT_PNUMBER
        ]:
            titel, facet, scope, bvn, uuid = clazz
            self._import_klasse(
                titel=titel,
                facet=facet,
                scope=scope,
                bvn=bvn,
                uuid=uuid
            )

    def _import_it_system(self):
        logger.info("Importing IT system")

        url = '{}/organisation/itsystem'.format(self.importer.mox_base)

        filename = 'Export_STAM_UUID_ITSystem.csv'
        rows = self._read_csv(filename)
        logger.info("Parsing {}".format(filename))
        logger.info("Importing {} IT systems".format(len(rows)))
        for row in rows:
            it_system = Itsystem(
                system_name=row['Name'],
                user_key=row['Userkey'],
            )
            it_system.organisation_uuid = self.org_uuid
            uuid = row['ITSystemUUID']

            json = it_system.build()
            r = requests.put('{}/{}'.format(url, uuid), json=json)
            r.raise_for_status()


    def _import_users(self):
        filename = 'Export_PDB_person.csv'
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)
        logger.info('Importing {} users'.format(len(rows)))
        for row in rows:
            # Create 'create' payloads
            # Create tasks
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

            end_date_raw = row.get('Slutdato', row.get('SlutDato'))

            if end_date_raw == '31-12-9999' or end_date_raw == 'NULL':
                end_date = None
            else:
                end_date = datetime.strptime(end_date_raw, '%d-%m-%Y')

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

    async def post_payloads(self, session, url, payload_lists):
        async def _post(payload_list):
            try:
                if type(payload_list) != list:
                    payload_list = [payload_list]
                for payload in payload_list:
                    async with session.post(url, json=payload) as r:
                        r.raise_for_status()
            except ClientResponseError as e:
                raise

        tasks = []
        for payload_list in payload_lists:
            task = asyncio.ensure_future(_post(payload_list))
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def submit_payloads(self, payload_lists, url, session):
        chunk_size = 100
        for i in range(0, len(payload_lists), chunk_size):
            print(i)
            chunk = payload_lists[i:i+chunk_size]
            await self.post_payloads(session, url, chunk)

    async def _import_employees(self, session):
        filename = 'Export_PDB_person.csv'
        logger.info("Parsing {}".format(filename))
        rows = self._read_csv(filename)
        logger.info('Importing {} users'.format(len(rows)))
        create_payloads = []
        for row in rows:
            payload = payloads.create_employee(
                givenname=row['Fornavn'],
                surname=row['Efternavn'],
                cpr_no=row['CPR'],
                uuid=row['PersonUUID']
            )
            create_payloads.append(payload)

        create_url = 'http://localhost:5000/service/e/create?force=1'
        logger.info('Importing {} employees'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)

    async def _import_pdb_units(self, session):
        unit_info = self._parse_file_with_uuid('Export_PDB_orgenhed.csv', 'OrgenhedUUID')

        create_payloads = []
        edit_payloads = {}
        edit_payload_count = 0
        for uuid, units in unit_info.items():
            first_unit = units[0]
            parent_uuid = first_unit['ParentOrgenhedUUID']
            if parent_uuid not in unit_info:
                logger.info('Parent "{}" not found'.format(parent_uuid))
                parent_uuid = self.org_uuid
            payload = payloads.create_org_unit(
            uuid=uuid,
                user_key=first_unit['OrgenhedID'],
                name=first_unit['Enhedsnavn'],
                parent_uuid=parent_uuid,
                org_unit_type_uuid=ORG_UNIT_TYPE_CACHE[first_unit['Enhedstype'].lower()],
                from_date=first_unit['from_date_mo'],
                to_date=first_unit['end_date_mo'],
            )
            create_payloads.append(payload)

            for unit in units[1:]:
                parent_uuid = unit['ParentOrgenhedUUID']
                if parent_uuid not in unit_info:
                    logger.info('Parent "{}" not found'.format(parent_uuid))
                    parent_uuid = self.org_uuid
                payload = payloads.edit_org_unit(
                    uuid=uuid,
                    user_key=unit['OrgenhedID'],
                    name=unit['Enhedsnavn'],
                    org_unit_type_uuid=ORG_UNIT_TYPE_CACHE[unit['Enhedstype'].lower()],
                    parent_uuid=parent_uuid,
                    from_date=unit['from_date_mo'],
                    to_date=unit['end_date_mo'],
                )
                edit_payloads.setdefault(uuid, []).append(payload)
                edit_payload_count += 1

        create_url = 'http://localhost:5000/service/ou/create?force=1'
        logger.info('Importing {} units'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)

        edit_url = 'http://localhost:5000/service/details/edit?force=1'
        logger.info('Performing {} unit updates'.format(edit_payload_count))
        await self.submit_payloads(list(edit_payloads.values()), edit_url, session)

    async def _import_los_units(self, session):
        unit_info = self._parse_file_with_uuid('Export_LOS_orgenhed.csv', uuid_field='LOSUUID')

        parent_map = {
            item[1][0]['ADM_ENHEDS_ID']: item[0]
            for item in unit_info.items()
        }

        def create_payload_dict(unit):
            parent_ref = unit['ORG_REFERENCE_TIL']
            parent_uuid = parent_map.get(parent_ref)
            if not parent_uuid:
                logger.warning('Parent {} not known'.format(parent_ref))
                parent_uuid = None
            if unit['ORG_NIVEAU'] == '1':
                parent_uuid = self.org_uuid

            org_unit_type_uuid = unit['EnhedstypeUUID']
            if org_unit_type_uuid == 'NULL':
                org_unit_type_uuid = UNKNOWN_ORG_UNIT_TYPE[-1]

            return {
                'uuid': uuid,
                'user_key': unit['KALDENAVN_KORT'],
                'name': unit['ADM_ENHEDS_NAVN'],
                'parent_uuid': parent_uuid,
                'org_unit_type_uuid': org_unit_type_uuid,
                'from_date': unit['from_date_mo'],
                'to_date': unit['end_date_mo']
            }

        skipped_units = [
            "616017cb-614d-455f-bcb7-6b950488fd8f",
            "59f4c3bf-f806-4ba3-8299-e01de4f4703f"
        ]

        def consolidate_payloads(payload_list):
            consolidated_payloads = []
            current_payload = payload_list[0]
            for payload in payload_list[1:]:
                current_to_date = datetime.strptime(current_payload['to_date'], "%Y-%m-%d") + timedelta(days=1)
                new_from_date = datetime.strptime(payload['from_date'], "%Y-%m-%d")

                if (
                    current_payload['name'] == payload['name'] and
                    current_payload['user_key'] == payload['user_key'] and
                    current_payload['parent_uuid'] == payload['parent_uuid'] and
                    current_payload['org_unit_type_uuid'] == payload['org_unit_type_uuid'] and
                    current_to_date == new_from_date
                ):
                    current_payload['to_date'] = payload['to_date']
                else:
                    consolidated_payloads.append(current_payload)
                    current_payload = payload
            consolidated_payloads.append(current_payload)
            return consolidated_payloads

        create_payloads = []
        edit_payloads = {}
        edit_payload_count = 0
        for uuid, units in unit_info.items():
            if uuid in skipped_units:
                logger.warning('Skipping {} as it is parentless'.format(uuid))
                continue
            payload_dicts = [create_payload_dict(unit) for unit in units]

            payload_dicts = consolidate_payloads(payload_dicts)

            create, *edits = payload_dicts

            create_payloads.append(payloads.create_org_unit(**create))
            for edit in edits:
                edit_payloads.setdefault(uuid, []).append(payloads.edit_org_unit(**edit))
                edit_payload_count += 1

        create_url = 'http://localhost:5000/service/ou/create?force=1'
        logger.info('Importing {} units'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)


        edit_url = 'http://localhost:5000/service/details/edit?force=1'
        logger.info('Performing {} unit updates'.format(edit_payload_count))
        await self.submit_payloads(list(edit_payloads.values()), edit_url, session)

    async def _import_los_addresses(self, session):
        unit_info = self._parse_file_with_uuid('Export_LOS_orgenhed.csv', uuid_field='LOSUUID')

        adresses = [
            (UNIT_DAR, 'AdresseUUID'),
            (UNIT_CVR, 'CVR'),
            (UNIT_EAN, 'EAN_LOKATIONS_NR'),
            (UNIT_PNUMBER, 'PROD_ENHED_NR'),
        ]

        def create_payload_dict(unit_row, org_unit_uuid, address_type_uuid, value_field):
            value = unit_row[value_field]
            if value == 'NULL':
                value = ''

            # Generate a UUID based on the unit/address_type combo so we
            # can generate edits for the same object later
            uuid = self._generate_uuid(org_unit_uuid, value_field)

            return {
                'uuid': uuid,
                'value': value,
                'address_type_uuid': address_type_uuid,
                'org_unit_uuid': org_unit_uuid,
                'from_date': unit_row['from_date_mo'],
                'to_date': unit_row['end_date_mo']
            }

        def consolidate_payloads(payload_list):
            consolidated_payloads = []
            current_payload = payload_list[0]
            for payload in payload_list[1:]:
                current_to_date = datetime.strptime(current_payload['to_date'], "%Y-%m-%d") + timedelta(days=1)
                new_from_date = datetime.strptime(payload['from_date'], "%Y-%m-%d")

                if current_payload['value'] == payload['value'] and current_to_date == new_from_date:
                    current_payload['to_date'] = payload['to_date']
                else:
                    consolidated_payloads.append(current_payload)
                    current_payload = payload
            consolidated_payloads.append(current_payload)
            return consolidated_payloads

        create_payloads = []
        edit_payloads = {}
        edit_payload_count = 0
        for address_type in adresses:
            address_tuple, address_field = address_type
            *_, address_type_uuid = address_tuple
            for uuid, units in unit_info.items():
                payload_dicts = [
                    create_payload_dict(unit_row, uuid, address_type_uuid, address_field)
                    for unit_row in units
                ]

                payload_dicts = consolidate_payloads(payload_dicts)

                if len(payload_dicts) == 1:
                    # If there is only a single row with no value, we just skip it
                    if payload_dicts[0]['value'] is None:
                        continue

                create, *edits = payload_dicts

                create_payloads.append(payloads.create_address(**create))
                for edit in edits:
                    edit_payloads.setdefault(uuid, []).append(payloads.edit_address(**edit))
                    edit_payload_count += 1

        create_url = 'http://localhost:5000/service/details/create?force=1'
        logger.info('Importing {} addresses'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)

        edit_url = 'http://localhost:5000/service/details/edit?force=1'
        logger.info('Performing {} address updates'.format(edit_payload_count))
        await self.submit_payloads(list(edit_payloads.values()), edit_url, session)

    async def _import_addresses(self, session):

        create_payloads = []
        edit_payloads = {}
        edit_payload_count = 0
        for address_tuple in ADDRESS_TUPLES:

            filename, fieldname, relname, address_type = address_tuple

            # This is the same format we use when creating the classes earlier
            address_title, address_facet, _, _, address_type_uuid = address_type

            def uuid_fn(row):
                return self._generate_uuid(address_title, row[relname], fieldname)

            address_info = self._parse_file(filename, uuid_fn)

            for uuid, addresses in address_info.items():
                first_address = addresses[0]

                payload = payloads.create_address(
                    uuid=uuid,
                    address_type_uuid=address_type_uuid,
                    person_uuid=first_address[relname] if relname == 'PersonUUID' else None,
                    org_unit_uuid=first_address[relname] if relname == 'OrgenhedUUID' else None,
                    value=first_address[fieldname],
                    from_date=first_address['from_date_mo'],
                    to_date=first_address['end_date_mo']
                )
                create_payloads.append(payload)

                for address in addresses[1:]:
                    payload = payloads.edit_address(
                        uuid=uuid,
                        address_type_uuid=address_type_uuid,
                        person_uuid=address[ relname] if relname == 'PersonUUID' else None,
                        org_unit_uuid=address[ relname] if relname == 'OrgenhedUUID' else None,
                        value=address[fieldname],
                        from_date=address['from_date_mo'],
                        to_date=address['end_date_mo']
                    )
                    edit_payloads.setdefault(uuid, []).append(payload)
                    edit_payload_count += 1

        create_url = 'http://localhost:5000/service/details/create?force=1'
        logger.info('Importing {} addresses'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)

        edit_url = 'http://localhost:5000/service/details/edit?force=1'
        logger.info('Performing {} address updates'.format(edit_payload_count))
        await self.submit_payloads(list(edit_payloads.values()), edit_url, session)

    async def _import_engagements(self, session):

        def uuid_fn(row):
            return self._generate_uuid(row['PersonUUID'], row['OrgenhedUUID'])

        engagement_info = self._parse_file('Export_PDB_person_rel.csv', uuid_fn)

        create_payloads = []
        edit_payloads = {}
        edit_payload_count = 0
        for uuid, engagements in engagement_info.items():
            first_engagement = engagements[0]
            payload = payloads.create_engagement(
                uuid=uuid,
                org_unit_uuid=first_engagement["OrgenhedUUID"],
                person_uuid=first_engagement['PersonUUID'],
                job_function_uuid=first_engagement['Stillingsbetegnelse'],
                engagement_type_uuid=first_engagement['Engagementstype'],
                from_date=first_engagement['from_date_mo'],
                to_date=first_engagement['end_date_mo'],
            )
            create_payloads.append(payload)

            for engagement in engagements[1:]:
                payload = payloads.edit_engagement(
                    uuid=uuid,
                    job_function_uuid=engagement['Stillingsbetegnelse'],
                    from_date=engagement['from_date_mo'],
                    to_date=engagement['end_date_mo'],
                )
                edit_payloads.setdefault(uuid, []).append(payload)
                edit_payload_count += 1

        create_url = 'http://localhost:5000/service/details/create?force=1'
        logger.info('Importing {} engagements'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)

        edit_url = 'http://localhost:5000/service/details/edit?force=1'
        logger.info('Performing {} engagement updates'.format(edit_payload_count))
        await self.submit_payloads(list(edit_payloads.values()), edit_url, session)

    async def _import_it(self, session):

        def uuid_fn(row):
            return self._generate_uuid(row['User_key'], row['ITSystemUUID'])

        it_rel_info = self._parse_file('Export_ITsystem_rel.csv', uuid_fn)

        def create_payload_dict(uuid, it_rel):
            return {
                'uuid': uuid,
                'user_key': it_rel['User_key'],
                'person_uuid': it_rel['PersonUUID'],
                'itsystem_uuid': it_rel['ITSystemUUID'],
                'from_date': it_rel['from_date_mo'],
                'to_date': it_rel['end_date_mo'],
            }

        create_payloads = []
        edit_payloads = {}
        edit_payload_count = 0
        for uuid, it_rels in it_rel_info.items():

            payload_dicts = [create_payload_dict(uuid, it_rel) for it_rel in it_rels]

            create, *edits = payload_dicts

            create_payloads.append(payloads.create_it_rel(**create))
            for edit in edits:
                edit_payloads.setdefault(uuid, []).append(payloads.edit_it_rel(**edit))
                edit_payload_count += 1


        create_url = 'http://localhost:5000/service/details/create?force=1'
        logger.info('Importing {} it relations'.format(len(create_payloads)))
        await self.submit_payloads(create_payloads, create_url, session)

        edit_url = 'http://localhost:5000/service/details/edit?force=1'
        logger.info('Performing {} it relation updates'.format(edit_payload_count))
        await self.submit_payloads(list(edit_payloads.values()), edit_url, session)


    async def initial_import(self):
        # Old sync importer code
        self._import_engagement_types()
        self._import_unit_types()
        self._import_job_functions()
        self._import_remaining_classes()
        self.importer.import_all()

        self.org_uuid = requests.get('http://localhost:5000/service/o/').json()[0]['uuid']

        self._import_it_system()

        connector = TCPConnector(limit=20)
        async with ClientSession(connector=connector) as session:
            await self._import_employees(session),
            await self._import_pdb_units(session),
            await self._import_los_units(session),
            await self._import_los_addresses(session),
            await self._import_engagements(session),
            await self._import_addresses(session),
            await self._import_it(session),


if __name__ == '__main__':
    # r = requests.get('http://localhost:8080/db/truncate')
    # r.raise_for_status()
    # print(r.text)

    århus = ÅrhusImport()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(århus.initial_import())
