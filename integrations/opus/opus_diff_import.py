# -- coding: utf-8 --
import json
import logging
import requests
import xmltodict

from pathlib import Path
from requests import Session
from datetime import datetime
from datetime import timedelta

from integrations import dawa_helper
from integrations.opus import payloads
from integrations.opus import opus_helpers
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.opus.calculate_primary import MOPrimaryEngagementUpdater
from integrations.opus.opus_exceptions import UnknownOpusUnit
from integrations.opus.opus_exceptions import EmploymentIdentifierNotUnique

logger = logging.getLogger("opusDiff")

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("opusImport")

for name in logging.root.manager.loggerDict:
    if name in ('opusImport', 'opusHelper', 'opusDiff', 'moImporterMoraTypes',
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


UNIT_ADDRESS_CHECKS = {
    'seNr': 'opus.addresses.unit.se',
    'cvrNr': 'opus.addresses.unit.cvr',
    'eanNr': 'opus.addresses.unit.ean',
    'pNr': 'opus.addresses.unit.pnr',
    'phoneNumber': 'opus.addresses.unit.phoneNumber',
    'dar': 'opus.addresses.unit.dar'
}


class OpusDiffImport(object):
    def __init__(self, latest_date, ad_reader, employee_mapping={}):
        logger.info('Opus diff importer __init__ started')
        # TODO: Soon we have done this 4 times. Should we make a small settings
        # importer, that will also handle datatype for specicic keys?
        cfg_file = Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.session = Session()
        self.employee_forced_uuids = employee_mapping
        self.ad_reader = ad_reader

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()
        # self.updater = MOPrimaryEngagementUpdater()

        self.engagement_types, _ = self._find_classes('engagement_type')
        self.unit_types, self.unit_type_facet = self._find_classes('org_unit_type')
        self.manager_levels, _ = self._find_classes('manager_level')
        self.role_types, _ = self._find_classes('role_type')
        self.manager_types, self.manager_type_facet = self._find_classes(
            'manager_type')
        self.responsibilities, _ = self._find_classes('responsibility')

        # TODO, this should also be done be _find_classes
        logger.info('Read job_functions')
        facet_info = self.helper.read_classes_in_facet('engagement_job_function')
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        self.job_functions = {}
        for job in job_functions:
            self.job_functions[job['name']] = job['uuid']

        self.role_cache = []
        units = self.helper._mo_lookup(self.org_uuid, 'o/{}/ou?limit=1000000000')
        for unit in units['items']:
            roles = self.helper._mo_lookup(unit['uuid'], 'ou/{}/details/role')
            for role in roles:
                self.role_cache.append(
                    {
                        'uuid': role['uuid'],
                        'person': role['person']['uuid'],
                        'validity': role['validity'],
                        'role_type': role['role_type']['uuid'],
                        'role_type_text': role['role_type']['name']
                    }
                )

        self.latest_date = latest_date
        self.units = None
        self.employees = None

    # This exact function also exists in sd_changed_at
    def _assert(self, response):
        """ Check response is as expected """
        assert response.status_code in (200, 400, 404)
        if response.status_code == 400:
            # Check actual response
            assert response.text.find('not give raise to a new registration') > 0
            logger.debug('Requst had no effect')
        return None

    def parser(self, target_file):
        data = xmltodict.parse(target_file.read_text())['kmd']
        self.units = data['orgUnit'][1:]
        self.employees = data['employee']
        return True

    def _update_unit_types(self, unit_type):
        unit_type_uuid = self.unit_types.get(unit_type)
        if unit_type_uuid is None:
            print('New unit type: {}!'.format(unit_type))
            response = self._add_klasse_to_lora(unit_type, self.unit_type_facet)
            uuid = response['uuid']
            self.unit_types[unit_type] = uuid

    def _get_organisationfunktion(self, lora_uuid):
        resource = '/organisation/organisationfunktion/{}'
        resource = resource.format(lora_uuid)
        response = self.session.get(url=self.settings['mox.base'] + resource)
        response.raise_for_status()
        data = response.json()
        data = data[lora_uuid][0]['registreringer'][0]
        # logger.debug('Organisationsfunktionsinfo: {}'.format(data))
        return data

    def _find_engagement(self, bvn, present=False):
        info = {}
        resource = '/organisation/organisationfunktion?bvn={}'.format(bvn)
        if present:
            resource += '&gyldighed=Aktiv'
        response = self.session.get(url=self.settings['mox.base'] + resource)
        response.raise_for_status()
        uuids = response.json()['results'][0]
        if uuids:
            if len(uuids) == 1:
                logger.info('bvn: {}, uuid: {}'.format(bvn, uuids))
                info['engagement'] = uuids[0]
            elif len(uuids) == 2:
                # This will happen if an exising manager is implicitly terminated
                for uuid in uuids:
                    org_funk = self._get_organisationfunktion(uuid)
                    org_funk_type = (
                        org_funk['attributter']
                        ['organisationfunktionegenskaber'][0]['funktionsnavn']
                    )
                    if org_funk_type == 'Engagement':
                        info['engagement'] = uuid
                    if org_funk_type == 'Leder':
                        info['manager'] = uuid

                if not ('manager' in info and 'engagement' in info):
                    msg = 'Found two uuids, but not of correct type'
                    logger.error(msg)
                    raise EmploymentIdentifierNotUnique(msg)
            elif len(uuids) > 2:
                msg = 'Employment ID {} not unique: {}'.format(bvn, uuids)
                logger.error(msg)
                raise EmploymentIdentifierNotUnique(msg)
        return info

    def _update_unit_addresses(self, unit):
        calculated_uuid = opus_helpers.generate_uuid(unit['@id'])
        unit_addresses = self.helper.read_ou_address(
            calculated_uuid, scope=None, return_all=True
        )

        address_dict = {}
        for address in unit_addresses:
            if address_dict.get(address['type']) is not None:
                # More than one of this type exist in MO, this is not allowed.
                msg = 'Inconsistent addresses for unit: {}'
                logger.error(msg.format(calculated_uuid))
            if address['value'] not in ('9999999999999', '0000000000'):
                address_dict[address['type']] = {
                    'value': address['value'],
                    'uuid': address['uuid']
                }

        if unit['street'] and unit['zipCode']:
            address_uuid = dawa_helper.dawa_lookup(unit['street'], unit['zipCode'])
            if address_uuid:
                logger.debug('Found DAR uuid: {}'.format(address_uuid))
                unit['dar'] = address_uuid
            else:
                logger.warning('Failed to lookup {}, {}'.format(unit['street'],
                                                                unit['zipCode']))

        for addr_type, setting in UNIT_ADDRESS_CHECKS.items():
            # addr_type is the opus name for the address, the MO uuid
            # for the corresponding class is found in settings.
            if unit.get(addr_type) is None:
                continue

            current = address_dict.get(self.settings[setting])
            args = {
                'address_type': {'uuid': self.settings[setting]},
                'value': unit[addr_type],
                'validity': {
                    'from': self.latest_date.strftime('%Y-%m-%d'),
                    'to': None
                },
                'unit_uuid': str(calculated_uuid)
            }
            self._perform_address_update(args, current, address_dict)

    def update_unit(self, unit):
        calculated_uuid = opus_helpers.generate_uuid(unit['@id'])
        parent_uuid = opus_helpers.generate_uuid(unit['parentOrgUnit'])
        mo_unit = self.helper.read_ou(calculated_uuid)

        # Default 'Enhed' is the default from the initial import
        org_type = unit.get('orgTypeTxt', 'Enhed')
        self._update_unit_types(org_type)
        unit_type = self.unit_types[org_type]

        unit_args = {
                'unit': unit,
                'unit_uuid': str(calculated_uuid),
                'unit_type': unit_type,
                'parent': str(parent_uuid),
                'from_date': self.latest_date.strftime('%Y-%m-%d')
        }

        if mo_unit.get('uuid'):  # Edit
            payload = payloads.edit_org_unit(**unit_args)
            logger.info('Edit unit: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            if response.status_code == 400:
                assert(response.text.find('raise to a new registration') > 0)
            else:
                response.raise_for_status()
        else:  # Create
            payload = payloads.create_org_unit(**unit_args)
            logger.debug('Create department payload: {}'.format(payload))
            response = self.helper._mo_post('ou/create', payload)
            response.raise_for_status()
            logger.info('Created unit {}'.format(unit['@id']))
            logger.debug('Response: {}'.format(response.text))

        self._update_unit_addresses(unit)

    def update_roller(self, employee):
        cpr = employee['cpr']['#text']
        mo_user = self.helper.read_user(user_cpr=cpr)
        logger.info('Check {} for updates in Roller'.format(cpr))
        if isinstance(employee['function'], dict):
            opus_roles = [employee['function']]
        else:
            opus_roles = employee['function']

        for opus_role in opus_roles:
            opus_end_datetime = datetime.strptime(opus_role['@endDate'], '%Y-%m-%d')
            if opus_role['@endDate'] == '9999-12-31':
                opus_role['@endDate'] = None

            found = False
            for mo_role in self.role_cache:
                if 'roleText' in opus_role:
                    combined_role = '{} - {}'.format(opus_role['artText'],
                                                     opus_role['roleText'])
                else:
                    combined_role = opus_role['artText']

                if (
                        mo_role['person'] == mo_user['uuid'] and
                        combined_role == mo_role['role_type_text']
                ):
                    found = True
                    if mo_role['validity']['to'] is None:
                        mo_end_datetime = datetime.strptime('9999-12-31', '%Y-%m-%d')
                    else:
                        mo_end_datetime = datetime.strptime(
                            mo_role['validity']['to'], '%Y-%m-%d'
                        )

                    # We only compare end dates, it is assumed start-date is not
                    # changed.
                    if mo_end_datetime == opus_end_datetime:
                        logger.info('No edit')
                    elif opus_end_datetime > mo_end_datetime:
                        logger.info('Extend role')
                        validity = {
                            'from': opus_role['@startDate'],
                            'to': opus_role['@endDate']
                        }
                        payload = payloads.edit_role(validity, mo_role['uuid'])
                        logger.debug('Edit role, payload: {}'.format(payload))
                        response = self.helper._mo_post('details/edit', payload)
                        self._assert(response)
                    else:  # opus_end_datetime < mo_end_datetime:
                        logger.info('Terminate role')
                        self.terminate_detail(
                            mo_role['uuid'],
                            detail_type='role',
                            end_date=opus_end_datetime
                        )
                    self.role_cache.remove(mo_role)
            if not found:
                logger.info('Create new role: {}'.format(opus_role))
                # TODO: We will fail a if  new role-type surfaces
                role_type = self.role_types.get(opus_role['artText'])
                payload = payloads.create_role(
                    employee=employee,
                    user_uuid=mo_user['uuid'],
                    unit_uuid=str(opus_helpers.generate_uuid(employee['orgUnit'])),
                    role_type=role_type,
                    validity={
                        'from': opus_role['@startDate'],
                        'to': opus_role['@endDate']
                    }
                )
                logger.debug('New role, payload: {}'.format(payload))
                response = self.helper._mo_post('details/create', payload)
                assert response.status_code == 201

    def update_employee(self, employee):
        cpr = employee['cpr']['#text']
        logger.info('----')
        logger.info('Now updating {}'.format(cpr))
        logger.debug('Available info: {}'.format(employee))
        mo_user = self.helper.read_user(user_cpr=cpr)

        if mo_user is None:
            employee_mo_uuid = self.create_user(employee)
        else:
            employee_mo_uuid = mo_user['uuid']
            if not ((employee['firstName'] == mo_user['givenname']) and
                    (employee['lastName'] == mo_user['surname'])):
                payload = payloads.create_user(employee, self.org_uuid,
                                               employee_mo_uuid)
                return_uuid = self.helper._mo_post('e/create', payload).json()
                msg = 'Updated name of employee {} with uuid {}'
                logger.info(msg.format(cpr, return_uuid))

        self._update_employee_address(employee_mo_uuid, employee)

        # Now we have a MO uuid, update engagement:
        mo_engagements = self.helper.read_user_engagement(employee_mo_uuid,
                                                          read_all=True)
        current_mo_eng = None
        for eng in mo_engagements:
            if eng['user_key'] == employee['@id']:
                current_mo_eng = eng['uuid']
                val_from = datetime.strptime(eng['validity']['from'], '%Y-%m-%d')
                if eng['validity']['to'] is None:
                    val_to = datetime.strptime('9999-12-31', '%Y-%m-%d')
                else:
                    val_to = datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
                if val_from < self.latest_date < val_to:
                    logger.info('Found current validty {}'.format(eng['validity']))
                    break

        if current_mo_eng is None:
            self.create_engagement(employee_mo_uuid, employee)
        else:
            logger.info('Validity for {}: {}'.format(employee['@id'],
                                                     eng['validity']))
            self.update_engagement(eng, employee)

        self.update_manager_status(employee_mo_uuid, employee)
        self.updater.set_current_person(cpr=cpr)
        self.updater.recalculate_primary()

    def terminate_detail(self, uuid, detail_type='engagement', end_date=None):
        if end_date is None:
            end_date = self.latest_date

        payload = payloads.terminate_detail(
            uuid, end_date.strftime('%Y-%m-%d'), detail_type
        )
        logger.debug('Terminate payload: {}'.format(payload))
        response = self.helper._mo_post('details/terminate', payload)
        logger.debug('Terminate response: {}'.format(response.text))
        self._assert(response)

    def start_re_import(self, xml_file, include_terminations=False):
        """
        Start an opus import, run the oldest available dump that
        has not already been imported.
        """
        self.parser(xml_file)

        for unit in self.units:
            last_changed = datetime.strptime(unit['@lastChanged'], '%Y-%m-%d')
            # Turns out org-unit updates are sometimes a day off
            last_changed = last_changed + timedelta(days=1)
            if last_changed > self.latest_date:
                self.update_unit(unit)

        for employee in self.employees:
            last_changed_str = employee.get('@lastChanged')
            if last_changed_str is not None:  # This is a true employee-object.
                last_changed = datetime.strptime(last_changed_str, '%Y-%m-%d')
                if last_changed > self.latest_date:
                    self.update_employee(employee)

                # Changes to Roller is not included in @lastChanged...
                if 'function' in employee:
                    self.update_roller(employee)
            else:  # This is an implicit termination.
                if not include_terminations:
                    continue

                # This is a terminated employee, check if engagement is active
                # terminate if it is.
                if not employee['@action'] == 'leave':
                    msg = 'Missing date on a non-leave object!'
                    logger.error(msg)
                    raise Exception(msg)

                org_funk_info = self._find_engagement(employee['@id'], present=True)
                if org_funk_info:
                    logger.info('Terminating: {}'.format(org_funk_info))
                    self.terminate_detail(org_funk_info['engagement'])
                    if 'manager' in org_funk_info:
                        self.terminate_detail(org_funk_info['manager'],
                                              detail_type='manager')

        for role in self.role_cache:
            logger.info('Role not found, implicitly terminating {}'.format(role))
            self.terminate_detail(role['uuid'], detail_type='role')

        logger.info('Program ended correctly')


if __name__ == '__main__':
    from integrations.ad_integration import ad_reader
    from integrations.opus.opus_helpers import start_opus_diff
    from integrations.opus.opus_exceptions import RunDBInitException

    cfg_file = Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    SETTINGS = json.loads(cfg_file.read_text())

    ad_reader = ad_reader.ADParameterReader()

    try:
        start_opus_diff(ad_reader=ad_reader)
    except RunDBInitException:
        print('RunDB not initialized')
