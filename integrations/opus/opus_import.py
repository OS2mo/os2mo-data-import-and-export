import json
import logging
import pathlib
from pathlib import Path

import xmltodict
from os2mo_helpers.mora_helpers import MoraHelper
from requests import Session

import constants
from ra_utils.load_settings import load_settings
from integrations import dawa_helper
from integrations.opus import opus_helpers
from integrations.opus.opus_exceptions import UnknownOpusAction

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("opusImport")

for name in logging.root.manager.loggerDict:
    if name in ('opusImport', 'opusHelper', 'moImporterMoraTypes',
                'moImporterMoxTypes', 'moImporterUtilities', 'moImporterHelpers',
                'ADReader'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class OpusImport(object):

    def __init__(self, importer, org_name, xml_data, ad_reader=None,
                 import_first=False, employee_mapping={}):
        """ If import first is False, the first unit will be skipped """
        self.org_uuid = None

        self.settings = load_settings()
        self.filter_ids = self.settings.get('integrations.opus.units.filter_ids', [])

        self.importer = importer
        self.import_first = import_first
        self.session = Session()
        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)

        self.organisation_id = None
        self.units = None
        self.employees = None
        # Update the above values
        municipality_code = self.parser(xml_data)

        self.org_name = org_name
        self.importer.add_organisation(
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        importer.new_itsystem(
            identifier='Opus',
            system_name=constants.Opus_it_system
        )

        self.ad_people = {}
        self.employee_forced_uuids = employee_mapping
        self.ad_reader = None
        if ad_reader:
            self.ad_reader = ad_reader
            self.importer.new_itsystem(
                identifier='AD',
                system_name=constants.AD_it_system
            )
            self.ad_reader.cache_all()

        self.employee_addresses = {}
        self._add_klasse('AddressPostUnit', 'Postadresse',
                         'org_unit_address_type', 'DAR')
        self._add_klasse('Pnummer', 'Pnummer',
                         'org_unit_address_type', 'PNUMBER')
        self._add_klasse('EAN', 'EAN', 'org_unit_address_type', 'EAN')
        self._add_klasse('PhoneUnit', 'Telefon', 'org_unit_address_type', 'PHONE')
        self._add_klasse('PhoneEmployee', 'Telefon', 'employee_address_type',
                         'PHONE')
        self._add_klasse('EmailEmployee', 'Email',
                         'employee_address_type', 'EMAIL')
        self._add_klasse('CVR', 'CVR', 'org_unit_address_type')
        self._add_klasse('SE', 'SE', 'org_unit_address_type')
        self._add_klasse('AdressePostEmployee', 'Postadresse',
                         'employee_address_type', 'DAR')
        self._add_klasse('Lederansvar', 'Lederansvar', 'responsibility')
        self._add_klasse('Ekstern', 'Må vises eksternt', 'visibility', 'PUBLIC')
        self._add_klasse('Intern', 'Må vises internt', 'visibility', 'INTERNAL')
        self._add_klasse('Hemmelig', 'Hemmelig', 'visibility', 'SECRET')

        self._add_klasse('primary', 'Ansat', 'primary_type', '3000')
        self._add_klasse('non-primary', 'Ikke-primær ansættelse',
                         'primary_type', '0')
        self._add_klasse('explicitly-primary', 'Manuelt primær ansættelse',
                         'primary_type', '5000')

    def _update_ad_map(self, cpr):
        logger.debug('Update cpr {}'.format(cpr))
        self.ad_people[cpr] = {}
        if self.ad_reader:
            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            if response:
                logger.debug('AD response: {}'.format(response))
                self.ad_people[cpr] = response
            else:
                logger.debug('Not found in AD')

    def insert_org_units(self):
        for unit in self.units:
            self._import_org_unit(unit)

    def insert_employees(self):
        for employee in self.employees:
            self._import_employee(employee)

    def _add_klasse(self, klasse_id, klasse, facet, scope='TEXT'):
        if not self.importer.check_if_exists('klasse', klasse_id):
            uuid = opus_helpers.generate_uuid(klasse_id)
            self.importer.add_klasse(
                identifier=klasse_id,
                uuid=uuid,
                facet_type_ref=facet,
                user_key=klasse_id,
                scope=scope,
                title=klasse
            )
        return klasse_id

    def parser(self, target_file):
        """
        Parse XML data and covert into usable dictionaries

        :return:
        """

        with open(target_file) as xmldump:
            data = xmltodict.parse(xmldump.read())['kmd']

        self.organisation_id = data['orgUnit'][0]['@id']

        if self.import_first:
            self.units = data['orgUnit']
        else:
            self.units = data['orgUnit'][1:]

        self.units = opus_helpers.filter_units(self.units, self.filter_ids)

        self.employees = data['employee']

        municipality_code = int(data['orgUnit'][0]['@client'])
        return municipality_code

    def _import_org_unit(self, unit):
        try:
            org_type = unit['orgType']
            self._add_klasse(org_type, unit['orgTypeTxt'], 'org_unit_type')
        except KeyError:
            org_type = 'Enhed'
            self._add_klasse(org_type, 'Enhed', 'org_unit_type')

        identifier = unit['@id']
        uuid = opus_helpers.generate_uuid(identifier)
        logger.debug('Generated uuid for {}: {}'.format(unit['@id'], uuid))

        user_key = unit['shortName']

        if unit['startDate'] == '1900-01-01':
            date_from = '1930-01-01'
        else:
            date_from = unit['startDate']

        if unit['endDate'] == '9999-12-31':
            date_to = None
        else:
            date_to = unit['endDate']

        name = unit['longName']

        parent_org = unit.get("parentOrgUnit")
        if parent_org == self.organisation_id and not self.import_first:
            parent_org = None

        self.importer.add_organisation_unit(
            identifier=identifier,
            name=name,
            uuid=str(uuid),
            user_key=user_key,
            parent_ref=parent_org,
            type_ref=org_type,
            date_from=date_from,
            date_to=date_to
        )

        if 'seNr' in unit:
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['seNr'],
                type_ref='SE',
                date_from=date_from,
                date_to=date_to
            )

        if 'cvrNr' in unit:
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['cvrNr'],
                type_ref='CVR',
                date_from=date_from,
                date_to=date_to
            )

        if 'eanNr' in unit and (not unit['eanNr'] == '9999999999999'):
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['eanNr'],
                type_ref='EAN',
                date_from=date_from,
                date_to=date_to
            )

        if 'pNr' in unit and (not unit['pNr'] == '0000000000'):
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['pNr'],
                type_ref='Pnummer',
                date_from=date_from,
                date_to=date_to
            )

        if unit['phoneNumber']:
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['phoneNumber'],
                type_ref='PhoneUnit',
                date_from=date_from,
                date_to=date_to
            )

        address_string = unit['street']
        zip_code = unit['zipCode']
        if address_string and zip_code:
            address_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
            if address_uuid:
                self.importer.add_address_type(
                    organisation_unit=identifier,
                    value=address_uuid,
                    type_ref='AddressPostUnit',
                    date_from=date_from,
                    date_to=date_to
                )

    def add_addresses_to_employees(self):
        for cpr, employee_addresses in self.employee_addresses.items():
            for facet, address in employee_addresses.items():
                logger.debug('Add address {}'.format(address))
                if address:
                    self.importer.add_address_type(
                        employee=cpr,
                        value=address,
                        type_ref=facet,
                        date_from='1930-01-01',
                        date_to=None
                    )

    def _import_employee(self, employee):
        # UNUSED KEYS:
        # '@lastChanged'

        logger.debug('Employee object: {}'.format(employee))
        if 'cpr' in employee:
            cpr = employee['cpr']['#text']
            if employee['firstName'] is None and employee['lastName'] is None:
                # Service user, skip
                logger.info('Skipped {}, we think it is a serviceuser'.format(cpr))
                return

        else:  # This employee has left the organisation
            if not employee['@action'] == 'leave':
                msg = 'Unknown action: {}'.format(employee['@action'])
                logger.error(msg)
                raise UnknownOpusAction(msg)
            return

        self._update_ad_map(cpr)

        uuid = self.employee_forced_uuids.get(cpr)
        logger.info('Employee in force list: {} {}'.format(cpr, uuid))
        if uuid is None and 'ObjectGuid' in self.ad_people[cpr]:
            uuid = self.ad_people[cpr]['ObjectGuid']

        date_from = employee['entryDate']
        date_to = employee['leaveDate']
        if date_from is None:
            date_from = '1930-01-01'

        # Only add employee and address information once, this info is duplicated
        # if the employee has multiple engagements
        if not self.importer.check_if_exists('employee', cpr):
            self.employee_addresses[cpr] = {}
            self.importer.add_employee(
                identifier=cpr,
                name=(employee['firstName'], employee['lastName']),
                cpr_no=cpr,
                uuid=uuid
            )

        if 'SamAccountName' in self.ad_people[cpr]:
            self.importer.join_itsystem(
                employee=cpr,
                user_key=self.ad_people[cpr]['SamAccountName'],
                itsystem_ref='AD',
                date_from='1930-01-01'
            )

        if 'userId' in employee:
            self.importer.join_itsystem(
                employee=cpr,
                user_key=employee['userId'],
                itsystem_ref='Opus',
                date_from=date_from,
                date_to=date_to
            )

        if 'email' in employee:
            self.employee_addresses[cpr]['EmailEmployee'] = employee['email']
        if employee['workPhone'] is not None:
            phone = opus_helpers.parse_phone(employee['workPhone'])
            self.employee_addresses[cpr]['PhoneEmployee'] = phone

        if 'postalCode' in employee and employee['address']:
            if isinstance(employee['address'], dict):
                # This is a protected address, cannot import
                pass
            else:
                address_string = employee['address']
                zip_code = employee["postalCode"]
                addr_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
                if addr_uuid:
                    self.employee_addresses[cpr]['AdressePostEmployee'] = addr_uuid

        job = employee["position"]
        self._add_klasse(job, job, 'engagement_job_function')

        if 'workContractText' in employee:
            contract = employee['workContractText']
        else:
            contract = 'Ansat'
        self._add_klasse(contract, contract, 'engagement_type')

        org_unit = employee['orgUnit']
        job_id = employee['@id']
        # engagement_uuid = opus_helpers.generate_uuid(job_id)

        # Every engagement is initially imported as non-primary,
        # a seperate script will correct this after import.
        # This allows separate rules for primary calculation.
        logger.info('Add engagement: {} to {}'.format(job_id, cpr))
        self.importer.add_engagement(
            employee=cpr,
            # uuid=str(engagement_uuid),
            organisation_unit=org_unit,
            primary_ref='non-primary',
            user_key=job_id,
            job_function_ref=job,
            engagement_type_ref=contract,
            date_from=date_from,
            date_to=date_to
        )

        if employee['isManager'] == 'true':
            manager_type_ref = 'manager_type_' + job
            self._add_klasse(manager_type_ref, job, 'manager_type')

            # Opus has two levels of manager_level, since MO handles only one
            # they are concatenated into one.
            manager_level = '{}.{}'.format(employee['superiorLevel'],
                                           employee['subordinateLevel'])
            self._add_klasse(manager_level, manager_level, 'manager_level')
            logger.info('{} is manager {}'.format(cpr, manager_level))
            self.importer.add_manager(
                employee=cpr,
                user_key=job_id,
                organisation_unit=org_unit,
                manager_level_ref=manager_level,
                manager_type_ref=manager_type_ref,
                responsibility_list=['Lederansvar'],
                date_from=date_from,
                date_to=date_to
            )

        if 'function' in employee:
            if not isinstance(employee['function'], list):
                roles = [employee['function']]
            else:
                roles = employee['function']

            for role in roles:
                logger.debug('{} has role {}'.format(cpr, role))
                # We have only a single class for roles, must combine the information
                if 'roleText' in role:
                    combined_role = '{} - {}'.format(role['artText'],
                                                     role['roleText'])
                else:
                    combined_role = role['artText']
                self._add_klasse(combined_role, combined_role, 'role_type')

                date_from = role['@startDate']
                if role['@endDate'] == '9999-12-31':
                    date_to = None
                else:
                    date_to = role['@endDate']

                self.importer.add_role(
                    employee=cpr,
                    organisation_unit=org_unit,
                    role_type_ref=combined_role,
                    date_from=date_from,
                    date_to=date_to
                )

def start_opus_import(importer, ad_reader=None, force=False):
    """
    Start an opus import, run the oldest available dump that
    has not already been imported.
    """
    SETTINGS = load_settings()
    dumps = opus_helpers.read_available_dumps()

    run_db = Path(SETTINGS['integrations.opus.import.run_db'])
    if not run_db.is_file():
        logger.error('Local base not correctly initialized')
        if not force:
            raise RunDBInitException('Local base not correctly initialized')
        else:
            opus_helpers.initialize_db(run_db)
        xml_date = sorted(dumps.keys())[0]
    else:
        if force:
            raise RedundantForceException('Used force on existing db')
        xml_date = opus_helpers.next_xml_file(run_db, dumps)

    xml_file = dumps[xml_date]
    opus_helpers.local_db_insert((xml_date, 'Running since {}'))

    employee_mapping = opus_helpers.read_cpr_mapping()

    opus_importer = OpusImport(
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

    opus_helpers.local_db_insert((xml_date, 'Import ended: {}'))
