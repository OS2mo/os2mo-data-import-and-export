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

EMPLOYEE_ADDRESS_CHECKS = {
    'phone': 'opus.addresses.employee.phone',
    'email': 'opus.addresses.employee.email',
    'dar': 'opus.addresses.employee.dar'
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
        self.updater = MOPrimaryEngagementUpdater()

        self.engagement_types, _ = self._find_classes('engagement_type')
        self.unit_types, self.unit_type_facet = self._find_classes('org_unit_type')
        self.manager_levels, self.manager_level_facet = self._find_classes(
            'manager_level')
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

        logger.info('Read Roles')
        # Potential to cut ~30s by parsing this:
        # /organisationfunktion?funktionsnavn=Rolle&virkningFra=2019-01-01
        self.role_cache = []
        units = self.helper._mo_lookup(self.org_uuid, 'o/{}/ou?limit=1000000000')
        for unit in units['items']:
            for validity in ['past', 'present', 'future']:
                url = 'ou/{}/details/role?validity=' + validity
                roles = self.helper._mo_lookup(unit['uuid'], url)
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
        logger.info('__init__ done, now start export')

    def _find_classes(self, facet):
        class_types = self.helper.read_classes_in_facet(facet)
        types_dict = {}
        facet = class_types[1]
        for class_type in class_types[0]:
            types_dict[class_type['user_key']] = class_type['uuid']
        return types_dict, facet

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

    def _add_klasse_to_lora(self, klasse_name, facet_uuid):
        klasse_uuid = opus_helpers.generate_uuid(klasse_name)
        logger.debug('Adding Klasse: {}, uuid: {}'.format(klasse_name, klasse_uuid))
        payload = payloads.klasse(klasse_name, self.org_uuid, facet_uuid)
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
            response = self._add_klasse_to_lora(emp_name, self.job_function_facet)
            uuid = response['uuid']
            self.job_functions[emp_name] = uuid

    def _update_unit_types(self, unit_type):
        unit_type_uuid = self.unit_types.get(unit_type)
        if unit_type_uuid is None:
            print('New unit type: {}!'.format(unit_type))
            response = self._add_klasse_to_lora(unit_type, self.unit_type_facet)
            uuid = response['uuid']
            self.unit_types[unit_type] = uuid

    def _update_manager_types(self, manager_type):
        manager_type_uuid = self.manager_types.get(manager_type)
        if manager_type_uuid is None:
            print('New manager type: {}!'.format(manager_type))
            response = self._add_klasse_to_lora(manager_type,
                                                self.manager_type_facet)
            uuid = response['uuid']
            self.manager_types[manager_type] = uuid

    def _update_manager_level(self, manager_level):
        manager_level_uuid = self.manager_levels.get(manager_level)
        if manager_level_uuid is None:
            print('New manager level: {}!'.format(manager_level))
            response = self._add_klasse_to_lora(manager_level,
                                                self.manager_level_facet)
            uuid = response['uuid']
            self.manager_levels[manager_level] = uuid

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

    def validity(self, employee, edit=False):
        """
        Calculates a validity object from en employee object.
        :param employee: An Opus employee object.
        :param edit: If True from will be current dump date, if true
        from will be taken from emploee object.
        :return: A valid MO valididty payload
        """
        to_date = employee['leaveDate']
        # if to_date is None: # This can most likely be removed
        #     to_datetime = datetime.strptime('9999-12-31', '%Y-%m-%d')
        # else:
        #     to_datetime = datetime.strptime(to_date, '%Y-%m-%d')

        from_date = employee['entryDate']
        if from_date is None:
            from_date = employee.get('@lastChanged')
        if not edit and from_date is None:
            logger.error('Missing start date for employee!')
            from_date = employee.get('@lastChanged')

        if edit:
            lastchanged = employee.get('@lastChanged')
            entry_datetime = datetime.strptime(from_date, '%Y-%m-%d')
            lastchanged_datetime = datetime.strptime(lastchanged, '%Y-%m-%d')

            if lastchanged_datetime > entry_datetime:
                from_date = lastchanged

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

        engagement_unit = self.helper.read_ou(unit_uuid)
        if 'error' in engagement_unit:
            msg = 'The wanted unit does not exit: {}'
            logger.error(msg.format(unit_uuid))
            raise UnknownOpusUnit

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

        if new_valid_to < old_valid_to:
            self.terminate_detail(
                engagement['uuid'],
                detail_type='engagement',
                end_date=new_valid_to
            )
        return something_new

    def create_engagement(self, mo_user_uuid, opus_employee):
        job_function, eng_type = self._job_and_engagement_type(opus_employee)
        unit_uuid = opus_helpers.generate_uuid(opus_employee['orgUnit'])

        engagement_unit = self.helper.read_ou(unit_uuid)
        if 'error' in engagement_unit:
            msg = 'The wanted unit does not exit: {}'
            logger.error(msg.format(opus_employee['orgUnit']))
            raise UnknownOpusUnit

        validity = self.validity(opus_employee, edit=False)
        payload = payloads.create_engagement(
            employee=opus_employee,
            user_uuid=mo_user_uuid,
            unit_uuid=unit_uuid,
            job_function=job_function,
            engagement_type=eng_type,
            primary=self.updater.primary_types['non_primary'],
            validity=validity
        )
        logger.debug('Create engagement payload: {}'.format(payload))
        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

    def create_user(self, employee):
        cpr = employee['cpr']['#text']
        if self.ad_reader is not None:
            ad_info = self.ad_reader.read_user(cpr=cpr)
        else:
            ad_info = {}
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

        if 'userId' in employee:
            payload = payloads.connect_it_system_to_user(
                employee['userId'],
                self.settings['integrations.opus.it_systems.opus'],
                return_uuid
            )
            logger.debug('Opus account payload: {}'.format(payload))
            response = self.helper._mo_post('details/create', payload)
            assert response.status_code == 201
            logger.info('Added Opus account info to {}'.format(cpr))

        sam_account = ad_info.get('SamAccountName', None)
        if sam_account:
            payload = payloads.connect_it_system_to_user(
                sam_account,
                self.settings['integrations.opus.it_systems.ad'],
                return_uuid
            )
            logger.debug('AD account payload: {}'.format(payload))
            response = self.helper._mo_post('details/create', payload)
            assert response.status_code == 201
            logger.info('Added AD account info to {}'.format(cpr))
        return return_uuid

    def _to_datetime(self, item):
        if item is None:
            item_datetime = datetime.strptime('9999-12-31', '%Y-%m-%d')
        else:
            item_datetime = datetime.strptime(item, '%Y-%m-%d')
        return item_datetime

    def update_manager_status(self, employee_mo_uuid, employee):
        url = 'e/{}/details/manager?at=' + self.validity(employee,
                                                         edit=True)["from"]
        manager_functions = self.helper._mo_lookup(employee_mo_uuid, url)
        logger.debug('Manager functions to update: {}'.format(manager_functions))
        if manager_functions:
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
            self._update_manager_level(manager_level)
            manager_level_uuid = self.manager_levels.get(manager_level)
            manager_type = 'manager_type_' + employee["position"]
            self._update_manager_types(manager_type)
            manager_type_uuid = self.manager_types.get(manager_type)
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

                mf = manager_functions[0]

                payload = payloads.edit_manager(
                    object_uuid=manager_functions[0]['uuid'],
                    **args
                )

                something_new = not (
                        mf['org_unit']['uuid'] == args['unit'] and
                        mf['person']['uuid'] == args['person'] and
                        mf['manager_type']['uuid'] == args['manager_type'] and
                        mf['manager_level']['uuid'] == args['level'] and
                        mf['responsibility'][0]['uuid'] == args['responsibility']
                )

                if something_new:
                    logger.debug('Something is changed, execute payload')
                else:
                    mo_end_datetime = self._to_datetime(mf['validity']['to'])
                    opus_end_datetime = self._to_datetime(args['validity']['to'])
                    logger.info('MO end datetime: {}'.format(mo_end_datetime))
                    logger.info('OPUS end datetime: {}'.format(opus_end_datetime))

                    if mo_end_datetime == opus_end_datetime:
                        logger.info('No edit of manager object')
                        payload = None
                    elif opus_end_datetime > mo_end_datetime:
                        logger.info('Extend validity, send payload to MO')
                    else:  # opus_end_datetime < mo_end_datetime:
                        logger.info('Terminate mangement role')
                        payload = None
                        self.terminate_detail(
                            mf['uuid'],
                            detail_type='manager',
                            end_date=opus_end_datetime
                        )

                logger.debug('Update manager payload: {}'.format(payload))
                if payload is not None:
                    response = self.helper._mo_post('details/edit', payload)
                    self._assert(response)
            else:  # No existing manager functions
                logger.info('Turn this person into a manager')
                # Validity is set to edit=True since the validiy should
                # calculated as an edit to the engagement
                payload = payloads.create_manager(user_key=employee['@id'], **args)
                logger.debug('Create manager payload: {}'.format(payload))
                response = self.helper._mo_post('details/create', payload)
                assert response.status_code == 201

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

    def import_single_employment(self, employee):
        # logger.info('Update  employment {} from {}'.format(employment, xml_file))
        last_changed_str = employee.get('@lastChanged')
        if last_changed_str is not None:  # This is a true employee-object.
            self.update_employee(employee)

            if 'function' in employee:
                self.update_roller(employee)
            else:
                # Terminate existing roles
                mo_user = self.helper.read_user(user_cpr=employee['cpr']['#text'])
                for role in self.role_cache:
                    if role['person'] == mo_user['uuid']:
                        logger.info('Terminating role: {}'.format(role))
                        self.terminate_detail(role['uuid'], detail_type='role')
        else:  # This is an implicit termination.
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
