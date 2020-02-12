import json
import logging
import pathlib
import requests

from datetime import datetime
from requests import Session

from integrations import dawa_helper
from integrations.opus import payloads
from integrations.opus import opus_helpers
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.opus.opus_exceptions import UnknownOpusUnit
from integrations.opus.calculate_primary import MOPrimaryEngagementUpdater

logger = logging.getLogger('OpusDiffCommon')

EMPLOYEE_ADDRESS_CHECKS = {
    'phone': 'opus.addresses.employee.phone',
    'email': 'opus.addresses.employee.email',
    'dar': 'opus.addresses.employee.dar'
}


class OpusDiffCommon(object):
    def __init__(self, latest_date, ad_reader, employee_mapping={}):
        logger.info('Opus diff importer __init__ started')
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
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
        self.role_types, _ = self._find_classes('role_type')
        self.responsibilities, _ = self._find_classes('responsibility')
        self.manager_levels, self.manager_level_facet = self._find_classes(
            'manager_level')
        self.manager_types, self.manager_type_facet = self._find_classes(
            'manager_type')

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

    def _find_classes(self, facet):
        class_types = self.helper.read_classes_in_facet(facet)
        types_dict = {}
        facet = class_types[1]
        for class_type in class_types[0]:
            types_dict[class_type['user_key']] = class_type['uuid']
        return types_dict, facet

    def _to_datetime(self, item):
        if item is None:
            item_datetime = datetime.strptime('9999-12-31', '%Y-%m-%d')
        else:
            item_datetime = datetime.strptime(item, '%Y-%m-%d')
        return item_datetime

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
            lastchanged = employee.get('@lastChanged', '2020-02-10')  # NOTICE!!!!!!!!!!!!!
            entry_datetime = datetime.strptime(from_date, '%Y-%m-%d')
            lastchanged_datetime = datetime.strptime(lastchanged, '%Y-%m-%d')

            if lastchanged_datetime > entry_datetime:
                from_date = lastchanged

        validity = {
            'from': from_date,
            'to': to_date
        }
        return validity

    def _update_manager_types(self, manager_type):
        manager_type_uuid = self.manager_types.get(manager_type)
        if manager_type_uuid is None:
            print('New manager type: {}!'.format(manager_type))
            if manager_type.find('manager_type_') == 0:
                manager_titel = manager_type[len('manager_type_'):]
            else:
                manager_titel = manager_type

            response = self._add_klasse_to_lora(
                klasse_name=manager_titel,
                klasse_bvn=manager_type,
                facet_uuid=self.manager_type_facet
            )
            uuid = response['uuid']
            self.manager_types[manager_type] = uuid

    def _update_manager_levels(self, manager_level):
        manager_level_uuid = self.manager_levels.get(manager_level)
        if manager_level_uuid is None:
            print('New manager level: {}!'.format(manager_level))
            response = self._add_klasse_to_lora(manager_level,
                                                self.manager_level_facet)
            uuid = response['uuid']
            self.manager_levels[manager_level] = uuid

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
        if 'workPhone' in employee and employee['workPhone'] is not None:
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

    def _add_klasse_to_lora(self, klasse_name, facet_uuid, klasse_bvn=None):
        if klasse_bvn is None:
            klasse_bvn = klasse_name
        klasse_uuid = opus_helpers.generate_uuid(klasse_name)
        logger.debug('Adding Klasse: {}, uuid: {}'.format(klasse_name, klasse_uuid))
        payload = payloads.klasse(klasse_name, klasse_bvn, self.org_uuid, facet_uuid)
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

    def _job_and_engagement_type(self, employee):
        # It is assumed no new engagement types are added during daily
        # updates. Default 'Ansat' is the default from the initial import
        job = employee["position"]
        self._update_professions(job)
        job_function = self.job_functions[job]

        contract = employee.get('workContractText', 'Ansat')
        eng_type = self.engagement_types[contract]
        return job_function, eng_type

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

    def update_manager_status(self, employee_mo_uuid, employee):
        url = 'e/{}/details/manager?at=' + self.latest_date.strftime('%Y-%m-%d')
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
            self._update_manager_levels(manager_level)
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

    def create_user(self, employee):
        cpr = employee['cpr']['#text']

        # NOTICE!
        # ad_info = self.ad_reader.read_user(cpr=cpr)
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
