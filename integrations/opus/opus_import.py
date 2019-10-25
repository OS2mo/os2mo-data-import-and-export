# -- coding: utf-8 --
import os
import uuid
import hashlib
import logging
import xmltodict

from requests import Session
from integrations.opus.opus_exceptions import UnknownOpusAction
from integrations.opus.opus_exceptions import EmploymentIdentifierNotUnique
from os2mo_helpers.mora_helpers import MoraHelper
from integrations import dawa_helper

MOX_BASE = os.environ.get('MOX_BASE')
MORA_BASE = os.environ.get('MORA_BASE', None)
LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("opusImport")

for name in logging.root.manager.loggerDict:
    if name in ('opusImport', 'moImporterMoraTypes', 'moImporterMoxTypes',
                'moImporterUtilities', 'moImporterHelpers', 'ADReader'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


def _parse_phone(phone_number):
    validated_phone = None
    if len(phone_number) == 8:
        validated_phone = phone_number
    elif len(phone_number) in (9, 11):
        validated_phone = phone_number.replace(' ', '')
    elif len(phone_number) in (4, 5):
        validated_phone = '0000' + phone_number.replace(' ', '')
    return validated_phone


class OpusImport(object):

    def __init__(self, importer, org_name, xml_data, ad_reader=None,
                 import_first=False):
        """ If import first is False, the first unit will be skipped """
        self.org_uuid = None
        self.importer = importer
        self.import_first = import_first
        self.session = Session()
        self.mox_base = MOX_BASE
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)

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
            system_name='Opus'
        )

        self.ad_people = {}
        if ad_reader:
            self.ad_reader = ad_reader
            self.importer.new_itsystem(
                identifier='AD',
                system_name='Active Directory'
            )
            self.ad_reader.cache_all()
        else:
            self.ad_reader = None

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

    def _generate_uuid(self, value):
        """
        Generate a semi-random, predictable uuid based on org name
        and a unique value.
        """
        base_hash = hashlib.md5(self.org_name.encode())
        base_digest = base_hash.hexdigest()
        base_uuid = uuid.UUID(base_digest)

        combined_value = (str(base_uuid) + str(value)).encode()
        value_hash = hashlib.md5(combined_value)
        value_digest = value_hash.hexdigest()
        value_uuid = uuid.UUID(value_digest)
        return value_uuid

    def _find_engagement(self, bvn, present=False):
        engagement_info = {}
        resource = '/organisation/organisationfunktion?bvn={}'.format(bvn)
        if present:
            resource += '&gyldighed=Aktiv'
        response = self.session.get(url=self.mox_base + resource)
        response.raise_for_status()
        uuids = response.json()['results'][0]
        if uuids:
            if len(uuids) > 1:
                msg = 'Employment ID {} not unique: {}'.format(bvn, uuids)
                logger.error(msg)
                raise EmploymentIdentifierNotUnique(msg)
            logger.info('bvn: {}, uuid: {}'.format(bvn, uuids))
            engagement_info['uuid'] = uuids[0]

            resource = '/organisation/organisationfunktion/{}'
            resource = resource.format(engagement_info['uuid'])
            response = self.session.get(url=self.mox_base + resource)
            response.raise_for_status()
            data = response.json()
            logger.debug('Organisationsfunktionsinfo: {}'.format(data))

            data = data[engagement_info['uuid']][0]['registreringer'][0]
            user_uuid = data['relationer']['tilknyttedebrugere'][0]['uuid']

            valid = data['tilstande']['organisationfunktiongyldighed']
            valid = valid[0]['gyldighed']
            if valid == 'Inaktiv':
                logger.debug('Inactive user, skip')
                return {}

            logger.debug('Active user, terminate')
            # Now, get user_key for user:
            if self.org_uuid is None:
                # We will get a hit unless this is a re-import, and in this case we
                # will always be able to find an org uuid.
                self.org_uuid = self.helper.read_organisation()
            mo_person = self.helper.read_user(user_uuid=user_uuid,
                                              org_uuid=self.org_uuid)
            engagement_info['cpr'] = mo_person['cpr_no']
            engagement_info['name'] = (mo_person['givenname'], mo_person['surname'])
        return engagement_info

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
            self.importer.add_klasse(identifier=klasse_id,
                                     facet_type_ref=facet,
                                     user_key=klasse,
                                     scope=scope,
                                     title=klasse)
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

        self.employees = data['employee']

        municipality_code = int(data['orgUnit'][0]['@client'])
        return municipality_code

    def _import_org_unit(self, unit):
        # UNUSED KEYS:
        # costCenter, @lastChanged

        try:
            org_type = unit['orgType']
            self._add_klasse(org_type, unit['orgTypeTxt'], 'org_unit_type')
        except KeyError:
            org_type = 'Enhed'
            self._add_klasse(org_type, 'Enhed', 'org_unit_type')

        identifier = unit['@id']
        uuid = self._generate_uuid(identifier)
        logger.debug('Generated uuid for {}: {}'.format(unit['@id'], uuid))

        user_key = unit['shortName']
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
                        date_from='1900-01-01',
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

            engagement_info = self._find_engagement(employee['@id'], present=True)
            if engagement_info:  # We need to add the employee for the sake of
                # the importers internal consistency
                if not self.importer.check_if_exists('employee',
                                                     engagement_info['cpr']):
                    self.importer.add_employee(
                        identifier=engagement_info['cpr'],
                        name=(engagement_info['name']),
                        cpr_no=engagement_info['cpr'],
                    )
                self.importer.terminate_engagement(
                    employee=engagement_info['cpr'],
                    engagement_uuid=engagement_info['uuid']
                )

            return

        self._update_ad_map(cpr)

        if 'ObjectGuid' in self.ad_people[cpr]:
            uuid = self.ad_people[cpr]['ObjectGuid']
        else:
            uuid = None

        date_from = employee['entryDate']
        date_to = employee['leaveDate']

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
                date_from=None
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
            phone = _parse_phone(employee['workPhone'])
            self.employee_addresses[cpr]['PhoneEmployee'] = phone

        if 'postalCode' in employee and employee['address']:
            if isinstance(employee['address'], dict):
                # TODO: This is a protected address
                # We currenly only support visibility for phones
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
            contract = employee['workContract']
            self._add_klasse(contract, employee['workContractText'],
                             'engagement_type')
        else:
            contract = '1'
            self._add_klasse(contract, 'Ansat', 'engagement_type')

        org_unit = employee['orgUnit']
        job_id = employee['@id']  # To be used soon
        logger.info('Add engagement: {} to {}'.format(job_id, cpr))
        self.importer.add_engagement(
            employee=cpr,
            organisation_unit=org_unit,
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
