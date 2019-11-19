# -- coding: utf-8 --
import json
import logging
import requests
import xmltodict

from pathlib import Path
from requests import Session
from datetime import datetime

from integrations import dawa_helper
from integrations.opus import payloads
from integrations.opus import opus_helpers
from os2mo_helpers.mora_helpers import MoraHelper
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

EMPLOYEE_ADDRESS_CHECKS = {
    'phone': 'opus.addresses.employee.phone',
    'email': 'opus.addresses.employee.email',
    'dar': 'opus.addresses.employee.dar'
}


class OpusDiffImport(object):
    def __init__(self, latest_date, ad_reader, employee_mapping={}):
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

        self.engagement_types = self._find_classes('engagement_type')
        self.unit_types = self._find_classes('org_unit_type')
        self.manager_levels = self._find_classes('manager_level')
        self.manager_types = self._find_classes('manager_type')
        self.responsibilities = self._find_classes('responsibility')

        logger.info('Read job_functions')
        facet_info = self.helper.read_classes_in_facet('engagement_job_function')
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        self.job_functions = {}
        for job in job_functions:
            self.job_functions[job['name']] = job['uuid']

        self.latest_date = latest_date
        self.units = None
        self.employees = None

    def _find_classes(self, facet):
        class_types = self.helper.read_classes_in_facet(facet)
        types_dict = {}
        for class_type in class_types[0]:
            types_dict[class_type['user_key']] = class_type['uuid']
        return types_dict

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

    def _add_profession_to_lora(self, profession):
        klasse_uuid = opus_helpers.generate_uuid(profession)
        logger.debug('Adding Klasse: {}, uuid: {}'.format(profession, klasse_uuid))
        payload = payloads.profession(profession, self.org_uuid,
                                      self.job_function_facet)
        url = '{}/klassifikation/klasse/{}'
        response = requests.put(
            url=url.format(self.settings['mox.base'], klasse_uuid),
            json=payload
        )
        assert response.status_code == 200
        return response.json()

    # This also exists in sd_changed_at
    def _update_professions(self, emp_name):
        job_uuid = self.job_functions.get(emp_name)
        if job_uuid is None:
            response = self._add_profession_to_lora(emp_name)
            uuid = response['uuid']
            self.job_functions[emp_name] = uuid

    def _find_engagement(self, bvn, present=False):
        engagement_info = {}
        resource = '/organisation/organisationfunktion?bvn={}'.format(bvn)
        if present:
            resource += '&gyldighed=Aktiv'
        response = self.session.get(url=self.settings['mox.base'] + resource)
        response.raise_for_status()
        uuids = response.json()['results'][0]
        if uuids:
            if len(uuids) > 1:
                # This will happen if an exising manager is implicitly terminated
                # waiting for this to happen before handling the case.
                print(uuids)
                print(len(uuids))
                msg = 'Employment ID {} not unique: {}'.format(bvn, uuids)
                logger.error(msg)
                raise EmploymentIdentifierNotUnique(msg)
            logger.info('bvn: {}, uuid: {}'.format(bvn, uuids))
            engagement_info['uuid'] = uuids[0]

            resource = '/organisation/organisationfunktion/{}'
            resource = resource.format(engagement_info['uuid'])
            response = self.session.get(url=self.settings['mox.base'] + resource)
            response.raise_for_status()
            data = response.json()
            # logger.debug('Organisationsfunktionsinfo: {}'.format(data))

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

    def validity(self, employee, edit=False):
        """
        Calculates a validity object from en employee object.
        :param employee: An Opus employee object.
        :param edit: If True from will be current dump date, if true
        from will be taken from emploee object.
        :return: A valid MO valididty payload
        """
        to_date = employee['leaveDate']
        if edit:
            # from_date = self.latest_date.strftime('%Y-%m-%d')
            from_date = employee.get('@lastChanged')
        else:
            from_date = employee['entryDate']

        validity = {
            'from': from_date,
            'to': to_date
        }
        return validity

    def _condense_employee_mo_addresses(self, mo_uuid):
        """
        Read all addresses from MO an return as a simple dict
        """
        # Unfortunately, mora-helper currently does not read all addresses
        user_addresses = self.helper._mo_lookup(mo_uuid, 'e/{}/details/address')
        address_dict = {}  # Condensate of all MO addresses for the employee
        for address in user_addresses:
            if address_dict.get(address['address_type']['uuid']) is not None:
                # More than one of this type exist in MO, this is not allowed.
                msg = 'Inconsistent addresses for employee: {}'
                logger.error(msg.format(mo_uuid))
            address_dict[address['address_type']['uuid']] = {
                'value': address['value'],
                'uuid': address['uuid']
            }
        return address_dict

    def _condense_employee_opus_addresses(self, employee):
        opus_addresses = {}
        if 'email' in employee:
            opus_addresses['email'] = employee['email']

        opus_addresses['phone'] = None
        if employee['workPhone'] is not None:
            phone = opus_helpers.parse_phone(employee['workPhone'])
            if phone is not None:
                opus_addresses['phone'] = phone

        if 'postalCode' in employee and employee['address']:
            if isinstance(employee['address'], dict):
                logger.info('Protected addres, cannont import')
            else:
                address_string = employee['address']
                zip_code = employee["postalCode"]
                addr_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
                if addr_uuid:
                    opus_addresses['dar'] = addr_uuid
                else:
                    logger.warning('Could not find address in DAR')
        return opus_addresses

    def _perform_address_update(self, args, current, mo_addresses):
        addr_type = args['address_type']['uuid']
        if current is None:  # Create address
            payload = payloads.create_address(**args)
            logger.debug('Create {} address payload: {}'.format(addr_type,
                                                                payload))
            response = self.helper._mo_post('details/create', payload)
            assert response.status_code == 201
        else:
            if current == mo_addresses.get(addr_type):  # Nothing changed
                logger.info('{} not updated'.format(addr_type))
            else:  # Edit address
                payload = payloads.edit_address(args, current['uuid'])
                logger.debug('Edit address {}, payload: {}'.format(addr_type,
                                                                   payload))
                response = self.helper._mo_post('details/edit', payload)
                self._assert(response)

    def _update_employee_address(self, mo_uuid, employee):
        opus_addresses = self._condense_employee_opus_addresses(employee)
        mo_addresses = self._condense_employee_mo_addresses(mo_uuid)
        logger.info('Addresses to be synced to MO: {}'.format(opus_addresses))

        for addr_type, setting in EMPLOYEE_ADDRESS_CHECKS.items():
            if opus_addresses.get(addr_type) is None:
                continue

            current = mo_addresses.get(self.settings[setting])
            address_args = {
                'address_type': {'uuid': self.settings[setting]},
                'value': opus_addresses[addr_type],
                'validity': {
                    'from': self.latest_date.strftime('%Y-%m-%d'),
                    'to': None
                },
                'user_uuid': mo_uuid
            }
            self._perform_address_update(address_args, current, mo_addresses)

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

        # It is assumed no new unit types are added during daily updates.
        # Default 'Enhed' is the default from the initial import
        org_type = unit.get('orgType', 'Enhed')
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

    def _job_and_engagement_type(self, employee):
        # It is assumed no new engagement types are added during daily
        # updates. Default 'Ansat' is the default from the initial import
        job = employee["position"]
        self._update_professions(job)
        job_function = self.job_functions[job]

        contract = employee.get('workContractText', 'Ansat')
        eng_type = self.engagement_types[contract]
        return job_function, eng_type

    def update_engagement(self, engagement, employee):
        """
        Update a MO engagement according to opus employee object.
        It often happens that the change that provoked lastChanged to
        be updated is not a MO field, and thus we check for relevant
        differences before shipping the payload to MO.
        :param engagement: Relevant MO engagement object.
        :param employee: Relevent Opus employee object.
        :return: True if update happended, False if not.
        """
        job_function, eng_type = self._job_and_engagement_type(employee)
        unit_uuid = opus_helpers.generate_uuid(employee['orgUnit'])
        validity = self.validity(employee, edit=True)
        data = {
            'engagement_type': {'uuid': eng_type},
            'job_function': {'uuid': job_function},
            'org_unit': {'uuid': str(unit_uuid)},
            'validity': validity
        }

        if engagement['validity']['to'] is None:
            old_valid_to = datetime.strptime('9999-12-31', '%Y-%m-%d')
        else:
            old_valid_to = datetime.strptime(engagement['validity']['to'],
                                             '%Y-%m-%d')
        if validity['to'] is None:
            new_valid_to = datetime.strptime('9999-12-31', '%Y-%m-%d')
        else:
            new_valid_to = datetime.strptime(validity['to'], '%Y-%m-%d')

        something_new = not (
            (engagement['engagement_type']['uuid'] == eng_type) and
            (engagement['job_function']['uuid'] == job_function) and
            (engagement['org_unit']['uuid'] == str(unit_uuid)) and
            (old_valid_to == new_valid_to)
        )

        logger.info('Something new? {}'.format(something_new))
        if something_new:
            payload = payloads.edit_engagement(data, engagement['uuid'])
            logger.debug('Update engagement payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)
        return something_new

    def create_engagement(self, mo_user_uuid, opus_employee):
        job_function, eng_type = self._job_and_engagement_type(opus_employee)
        unit_uuid = opus_helpers.generate_uuid(opus_employee['orgUnit'])
        validity = self.validity(opus_employee, edit=False)
        payload = payloads.create_engagement(
            employee=opus_employee,
            user_uuid=mo_user_uuid,
            unit_uuid=unit_uuid,
            job_function=job_function,
            engagement_type=eng_type,
            validity=validity
        )
        logger.debug('Create engagement payload: {}'.format(payload))
        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

    def create_user(self, employee):
        cpr = employee['cpr']['#text']
        ad_info = self.ad_reader.read_user(cpr=cpr)
        uuid = self.employee_forced_uuids.get(cpr)
        logger.info('Employee in force list: {} {}'.format(cpr, uuid))
        if uuid is None and cpr in ad_info:
            uuid = ad_info[cpr]['ObjectGuid']
            if uuid is None:
                msg = '{} not in MO, UUID list or AD, assign random uuid'
                logger.debug(msg.format(cpr))
        payload = payloads.create_user(employee, self.org_uuid, uuid)

        logger.info('Create user payload: {}'.format(payload))
        return_uuid = self.helper._mo_post('e/create', payload).json()
        logger.info('Created employee {} {} with uuid {}'.format(
            employee['firstName'],
            employee['lastName'],
            return_uuid
        ))

        # TODO: CHECK IF USER IS OPUS USER, AD IT SYSTEM IF SO!
        # No userIds in current dataset, this will have to wait.
        if 'userId' in employee:
            print(employee)
            exit()
        # self.settings['opus.it_systems.opus'],

        sam_account = ad_info.get('SamAccountName', None)
        if sam_account:
            payload = payloads.connect_it_system_to_user(
                sam_account,
                self.settings['opus.it_systems.ad'],
                return_uuid
            )
            logger.debug('AD account payload: {}'.format(payload))
            response = self.helper._mo_post('details/create', payload)
            assert response.status_code == 201
            logger.info('Added AD account info to {}'.format(cpr))
        return return_uuid

    def update_manager_status(self, employee_mo_uuid, employee):
        manager_functions = self.helper._mo_lookup(employee_mo_uuid,
                                                   'e/{}/details/manager')
        logger.debug('Manager functions to update: {}'.format(manager_functions))

        if employee['isManager'] == 'false':
            if manager_functions:
                logger.info('Terminate manager function')
                self.terminate_detail(manager_functions[0]['uuid'],
                                      detail_type='manager')
            else:
                logger.debug('Correctly not a manager')

        if employee['isManager'] == 'true':
            manager_level = '{}.{}'.format(employee['superiorLevel'],
                                           employee['subordinateLevel'])
            manager_level_uuid = self.manager_levels.get(manager_level)
            manager_type_uuid = self.manager_types.get(employee["position"])
            # This will fail if new manager levels or types are added...
            responsibility_uuid = self.responsibilities.get('Lederansvar')

            args = {
                'unit': str(opus_helpers.generate_uuid(employee['orgUnit'])),
                'person': employee_mo_uuid,
                'manager_type': manager_type_uuid,
                'level': manager_level_uuid,
                'responsibility': responsibility_uuid,
                'validity': self.validity(employee, edit=True)
            }

            if manager_functions:
                logger.info('Attempt manager update of {}:'.format(employee_mo_uuid))
                # Currently Opus supports only a single manager object pr employee
                assert len(manager_functions) == 1
                payload = payloads.edit_manager(
                    object_uuid=manager_functions[0]['uuid'],
                    **args
                )
                logger.debug('Update manager payload: {}'.format(payload))
                response = self.helper._mo_post('details/edit', payload)
                self._assert(response)
            else:
                logger.info('Turn this person into a manager')
                # Validity is set to edit=True since the validiy should
                # calculated as an edit to the engagement
                payload = payloads.create_manager(user_key=employee['@id'], **args)
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

        # TODO: Update Roller
        if 'function' in employee:
            print()
            print('Employee has a role')
            print(cpr)
            print(employee['function'])
            print()
            exit()

    def terminate_detail(self, uuid, detail_type='engagement'):
        payload = payloads.terminate_detail(
            uuid, self.latest_date.strftime('%Y-%m-%d'), detail_type
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
        logger.info('Program started')
        self.parser(xml_file)

        for unit in self.units:
            last_changed = datetime.strptime(unit['@lastChanged'], '%Y-%m-%d')
            if last_changed > self.latest_date:
                self.update_unit(unit)

        for employee in self.employees:
            last_changed_str = employee.get('@lastChanged')
            if last_changed_str is not None:
                # This is a true employee-object
                last_changed = datetime.strptime(last_changed_str, '%Y-%m-%d')
                if last_changed > self.latest_date:
                    self.update_employee(employee)
            else:
                if not include_terminations:
                    continue

                # This is a terminated employee, check if engagement is active
                # terminate if it is.
                if not employee['@action'] == 'leave':
                    msg = 'Missing date on a non-leave object!'
                    logger.error(msg)
                    raise Exception(msg)

                engagement = self._find_engagement(employee['@id'], present=True)
                if engagement:
                    self.terminate_detail(engagement['uuid'])
                    # self.terminate_detail(, detail_type=manager)
        logger.info('Program ended correctly')
