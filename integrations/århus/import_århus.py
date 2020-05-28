import csv
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

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_initial_import.log'

logger = logging.getLogger('århusImport')

for name in logging.root.manager.loggerDict:
    if name in ('moImporterMoraTypes', 'moImporterMoxTypes', 'moImporterUtilities',
                'moImporterHelpers'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)

UUID_ORG_DAR = '00000000-0000-0000-0000-000000000000'
UUID_PERSON_EMAIL = '00000000-0000-0000-0000-000000000001'


class ÅrhusImport(object):
    def __init__(self):
        self.csv_path = pathlib.Path('/home/robert/Downloads/Århus/csv_filer')

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
            municipality_code=751
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

    def _generate_uuid(self, relation, value):
        """
        Generates an uuid based on the uuid of a part of a relation (person or
        unit) and value.
        """
        base_hash = hashlib.md5(relation.encode())
        base_digest = base_hash.hexdigest()
        base_uuid = UUID(base_digest)

        combined_value = (str(base_uuid) + str(value)).encode()
        value_hash = hashlib.md5(combined_value)
        value_digest = value_hash.hexdigest()
        value_uuid = str(UUID(value_digest))
        return value_uuid

    def _import_klasse(self, titel, facet, scope='TEXT', bvn=None, uuid=None):
        if bvn is None:
            bvn = titel
        self.importer.add_klasse(
            identifier=titel + facet,
            uuid=uuid,
            facet_type_ref=facet,
            user_key=titel,
            scope=scope,
            title=titel
        )

    def _import_engagement_types(self):
        filename = 'Export_STAM_UUID_Engagementstype.csv'
        rows = self._read_csv(filename)
        for row in rows:
            print('Importing {}'.format(row['Engagementstype']))
            self._import_klasse(row['Engagementstype'], 'engagement_type',
                                uuid=row['EngagementstypeUUID'])

    def _import_unit_types(self):
        filename = 'Export_STAM_UUID_Enhedstype.csv'
        rows = self._read_csv(filename)
        for row in rows:
            print('Importing {}'.format(row['Enhedstype']))
            self._import_klasse(row['Enhedstype'], 'org_unit_type',
                                uuid=row['EnhedstypeUUID'])

    def _import_job_functions(self):
        filename = 'Export_STAM_UUID_Stillingsbetegnelse.csv'
        rows = self._read_csv(filename)
        for row in rows:
            print('Importing {}'.format(row['Stillingsbetegnelse']))
            self._import_klasse(row['Stillingsbetegnelse'],
                                'engagement_job_function',
                                uuid=row['StillingBetUUID'])

    def _import_remaining_classes(self):
        self._import_klasse('Postadresse', 'org_unit_address_type',
                            scope='DAR', bvn='AddressMailUnit',
                            uuid=UUID_ORG_DAR)

        self._import_klasse('Email', 'employee_address_type',
                            scope='EMAIL', bvn='EmailEmployee',
                            uuid=UUID_PERSON_EMAIL)

        self._import_klasse('Telefon 1', 'employee_address_type',
                            scope='PHONE', bvn='PhoneEmployee',
                            uuid='00000000-0000-0000-0000-000000000002')
        self._import_klasse('Telefon 2', 'employee_address_type',
                            scope='PHONE', bvn='EmailEmployee',
                            uuid='00000000-0000-0000-0000-000000000003')

    def _import_users(self):
        filename = 'Export_PDB_person.csv'
        rows = self._read_csv(filename)
        for row in rows:
            print('Importing: {} {}'.format(row['Fornavn'], row['Efternavn']))
            self.importer.add_employee(
                name=(row['Fornavn'], row['Efternavn']),
                identifier=row['PersonUUID'],
                cpr_no=row['CPR'],
                user_key=row['CPR'],
                uuid=row['PersonUUID']
            )

    def _parse_org_unit_file(self):
        # Todo, generalize to all objects with pre-defined uuid
        filename = 'Export_PDB_orgenhed.csv'
        rows = self._read_csv(filename)
        object_history = {}
        for row in rows:
            org_uuid = row['OrgenhedUUID']
            from_date = datetime.strptime(row['Startdato'], '%d-%m-%Y')
            if row['Slutdato'] == '31-12-9999':
                end_date = None
            else:
                end_date = datetime.strptime(row['Slutdato'], '%d-%m-%Y')

            row['from_date'] = from_date
            row['end_date'] = end_date
            row['from_date_mo'] = datetime.strftime(from_date, "%Y-%m-%d")
            if end_date is None:
                row['end_date_mo'] = None
            else:
                row['end_date_mo'] = datetime.strftime(end_date, "%Y-%m-%d")

            if org_uuid not in object_history:
                object_history[org_uuid] = [row]
            else:
                i = 0
                while object_history[org_uuid][i]['from_date'] < from_date:
                    i = i + 1
                    if i == len(object_history[org_uuid]):
                        break
                object_history[org_uuid].insert(i, row)
        # for row in object_history['16223af8-88cb-42ae-bdc9-cb9a50eeb573']:
        #    print('Fra: {}, til: {}'.format(row['from_date'], row['end_date']))
        return object_history

    def _parse_file_without_uuid(self, filename, relname, fieldname):
        print('Parsing {}'.format(filename))
        rows = self._read_csv(filename)
        object_history = {}
        for row in rows:
            uuid = self._generate_uuid(row[relname], fieldname)
            from_date = datetime.strptime(row['Startdato'], '%d-%m-%Y')
            if row['Slutdato'] == '31-12-9999':
                end_date = None
            else:
                end_date = datetime.strptime(row['Slutdato'], '%d-%m-%Y')
                if end_date < from_date:
                    print('Illegal row: {}!'.format(row))
                    continue

            row['from_date'] = from_date
            row['end_date'] = end_date
            row['from_date_mo'] = datetime.strftime(from_date, "%Y-%m-%d")
            if end_date is None:
                row['end_date_mo'] = None
            else:
                row['end_date_mo'] = datetime.strftime(end_date, "%Y-%m-%d")

            if uuid not in object_history:
                object_history[uuid] = [row]
            else:
                i = 0
                while object_history[uuid][i]['from_date'] < from_date:
                    i = i + 1
                    if i == len(object_history[uuid]):
                        break
                object_history[uuid].insert(i, row)
        return object_history

    def _initial_import_units(self):
        unit_info = self._parse_org_unit_file()
        for uuid, unit in unit_info.items():
            parent = unit[0]['ParentOrgenhedUUID']
            if parent not in unit_info:
                print('Parent not found')
                parent = None

            print('Importing {}'.format(unit[0]['Enhedsnavn']))
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

    def _initial_import_person_email(self):
        # Todo, generalize to read all adresses
        filename = 'Export_PDB_person_email.csv'
        fieldname = 'Email'
        relname = 'PersonUUID'
        mail_info = self._parse_file_without_uuid(filename, relname, fieldname)
        for uuid, mail in mail_info.items():
            print('Importing {} to {}'.format(mail[0]['Email'],
                                              mail[0]['PersonUUID']))
            self.importer.add_address_type(
                uuid=uuid,
                employee=mail[0]['PersonUUID'],
                type_ref='Emailemployee_address_type',
                value=mail[0]['Email'],
                date_from=mail[0]['from_date_mo'],
                date_to=mail[0]['end_date_mo']
            )

    # def _diff_import_org_unit(self):
    #     unit_info = self._parse_org_unit_file()
    #     for uuid, unit_row in unit_info.items():
    #         if len(unit_row) == 1:
    #             continue
    #         for unit in unit_row[1:]:
    #             parent = unit['ParentOrgenhedUUID']
    #             if parent not in unit_info:
    #                 print('Parent not found')
    #                 parent = None
    #             edit_payload = payloads.edit_org_uni(
    #                 user_key=,
    #                 name=,
    #                 unit_uuid=,
    #                 parent=,
    #                 ou_type='7F191604-BFBB-4205-A5C6-4B233AB3B7A3',
    #                 from_date, to_date
    #             )

    def _diff_import_person_email(self):
        # Todo, generalize to read all adresses
        filename = 'Export_PDB_person_email.csv'
        fieldname = 'Email'
        relname = 'PersonUUID'
        mail_info = self._parse_file_without_uuid(filename, relname, fieldname)
        for uuid, mail_row in mail_info.items():
            if len(mail_row) == 1:
                continue
            print('Mail row: {}'.format(mail_row))
            for mail in mail_row[1:]:
                address_args = {
                    'address_type': {'uuid': UUID_PERSON_EMAIL},
                    'value': mail['Email'],
                    'validity': {
                        'from': mail['from_date_mo'],
                        'to': mail['end_date_mo']
                    },
                    'user_uuid': mail['PersonUUID']
                }
                payload = payloads.edit_address(address_args, uuid)
                print(payload)
                response = self.helper._mo_post('details/edit', payload)
                response.raise_for_status()

    def initial_import(self):
        self._import_engagement_types()
        self._import_unit_types()
        self._import_job_functions()
        self._import_remaining_classes()
        self._import_users()
        self._initial_import_units()
        self._initial_import_person_email()

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

        self._diff_import_person_email()


if __name__ == '__main__':
    århus = ÅrhusImport()
    århus.initial_import()
    århus.diff_import()
