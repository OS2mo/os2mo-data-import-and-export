import os
import uuid
import json
import logging
import hashlib
import sqlite3
import requests

from pathlib import Path
from requests import Session
from datetime import datetime

from integrations import dawa_helper
from integrations.opus import payloads
from integrations.opus import opus_helpers
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.opus.opus_exceptions import EmploymentIdentifierNotUnique

RUN_DB = os.environ.get('RUN_DB')
MORA_BASE = os.environ.get('MORA_BASE')

logger = logging.getLogger("opusDiff")

ADDRESS_CHECKS = {
    'seNr': 'opus.addresses.se',
    'cvrNr': 'opus.addresses.cvr',
    'eanNr': 'opus.addresses.ean',
    'pNr': 'opus.addresses.pnr',
    'phoneNumber': 'opus.addresses.phoneNumber',
    'dar': 'opus.addresses.dar'  # DAR is a special case
}


class OpusImport(object):
    def __init__(self, ad_reader, employee_mapping={}):
        # TODO: Soon we have done this 4 times. Should we make a small settings
        # importer, that will also handle datatype for specicic keys?
        cfg_file = Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.session = Session()
        # TODO: Test with an actual employee mapping
        self.employee_forced_uuids = employee_mapping
        self.ad_reader = ad_reader

        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
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

        # THIS IS COMMON TO _next_xml_file!!!!
        conn = sqlite3.connect(RUN_DB, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        query = 'select * from runs order by id desc limit 1'
        c.execute(query)
        row = c.fetchone()
        self.latest_date = row[1]
        # THIS IS COMMON TO _next_xml_file!!!!

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

    # THIS IS A CRIPPLED COPY OF A FUNCTION IN OPUS_IMPORT!
    def parser(self, target_file):
        import xmltodict
        data = xmltodict.parse(target_file.read_text())['kmd']
        self.units = data['orgUnit'][1:]
        self.employees = data['employee']
        return True

    # COPY OF FUNCTIONIN OPUS_IMPORT!!!!!!!!!!
    def _generate_uuid(self, value):
        """
        Generate a semi-random, predictable uuid based on org name
        and a unique value.
        """
        base_hash = hashlib.md5(self.settings['municipality.name'].encode())
        base_digest = base_hash.hexdigest()
        base_uuid = uuid.UUID(base_digest)

        combined_value = (str(base_uuid) + str(value)).encode()
        value_hash = hashlib.md5(combined_value)
        value_digest = value_hash.hexdigest()
        value_uuid = uuid.UUID(value_digest)
        return value_uuid

    # This also exists in sd_changed_at
    def _add_profession_to_lora(self, profession):
        payload = payloads.profession(profession, self.org_uuid,
                                      self.job_function_facet)
        response = requests.post(
            url=self.settings['mox.base'] + '/klassifikation/klasse',
            json=payload
        )
        assert response.status_code == 201
        return response.json()

    # This also exists in sd_changed_at
    def _update_professions(self, emp_name):
        # Add new profssions to LoRa
        job_uuid = self.job_functions.get(emp_name)
        if job_uuid is None:
            response = self._add_profession_to_lora(emp_name)
            uuid = response['uuid']
            self.job_functions[emp_name] = uuid

    # This exists in opus_import. This is the correct position.
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
            from_date = self.latest_date.strftime('%Y-%m-%d')
        else:
            from_date = employee['entryDate']

        validity = {
            'from': from_date,
            'to': to_date
        }
        return validity

    def _update_unit_addresses(self, unit):
        calculated_uuid = self._generate_uuid(unit['@id'])
        unit_addresses = self.helper.read_ou_address(
            calculated_uuid, scope=None, return_all=True
        )

        address_dict = {}  # Condensate of all addresses in the unit
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

        for addr_type, setting in ADDRESS_CHECKS.items():
            # addr_type is the opus name for the address, the MO uuid
            # for the corresponding class is found in settings.
            if unit.get(addr_type) is None:
                continue

            current_value = address_dict.get(self.settings[setting])
            address_args = {
                'address_type': {'uuid': self.settings[setting]},
                'value': unit[addr_type],
                'validity': {
                    'from': self.latest_date.strftime('%Y-%m-%d'),
                    'to': None
                },
                'unit_uuid': str(calculated_uuid)
            }
            if current_value is None:  # Create address
                payload = payloads.create_address(**address_args)
                logger.debug('Create {} address payload: {}'.format(addr_type,
                                                                    payload))
                response = self.helper._mo_post('details/create', payload)
                assert response.status_code == 201
            else:
                if current_value['value'] == unit[addr_type]:  # Nothing changed
                    logger.info('{} not updated'.format(addr_type))
                else:  # Edit address
                    payload = payloads.edit_address(address_args,
                                                    current_value['uuid'])
                    logger.debug('Edit {} address payload: {}'.format(addr_type,
                                                                      payload))
                    response = self.helper._mo_post('details/edit', payload)
                    self._assert(response)

    def update_unit(self, unit):
        calculated_uuid = self._generate_uuid(unit['@id'])
        parent_uuid = self._generate_uuid(unit['parentOrgUnit'])
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

    def update_engagement(self, eng_uuid, employee):
        job_function, eng_type = self._job_and_engagement_type(employee)
        unit_uuid = self._generate_uuid(employee['orgUnit'])
        validity = self.validity(employee, edit=True)
        data = {
            'engagement_type': {'uuid': eng_type},
            'job_function': {'uuid': job_function},
            'org_unit': {'uuid': str(unit_uuid)},
            'validity': validity
        }
        payload = payloads.edit_engagement(data, eng_uuid)
        logger.debug('Update engagement payload: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        self._assert(response)

    def create_engagement(self, mo_user_uuid, opus_employee):
        job_function, eng_type = self._job_and_engagement_type(opus_employee)
        unit_uuid = self._generate_uuid(opus_employee['orgUnit'])
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

    def update_employee(self, employee):
        cpr = employee['cpr']['#text']
        logger.info('Now updating {}'.format(cpr))
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

            # TODO: Here We should update user address

        # Now we have a MO uuid, update engagement:
        mo_engagements = self.helper.read_user_engagement(employee_mo_uuid,
                                                          read_all=True)
        current_mo_eng = None
        for eng in mo_engagements:
            if eng['user_key'] == employee['@id']:
                current_mo_eng = eng['uuid']
                break

        if current_mo_eng is None:
            self.create_engagement(employee_mo_uuid, employee)
        else:
            self.update_engagement(current_mo_eng, employee)

        # Update manager information:
        manager_functions = self.helper._mo_lookup(employee_mo_uuid,
                                                   'e/{}/details/manager')

        if manager_functions and employee['isManager'] == 'false':
            print('Terminate manager function')
            # TODO

        if not manager_functions and employee['isManager'] == 'false':
            logger.debug('Correctly not a manager')

        if not manager_functions and employee['isManager'] == 'true':
            print('Turn this person into a manager')

        if manager_functions and employee['isManager'] == 'true':
            print('Still a manager - Check for potential edits')
            print('Attempt manager update of {}:'.format(employee_mo_uuid))
            # Currently Opus supports only a single manager object pr employee
            assert len(manager_functions) == 1
            mo_manager = manager_functions[0]
            manager_level = '{}.{}'.format(employee['superiorLevel'],
                                           employee['subordinateLevel'])
            # This will fail if new manager levels or types are added...
            manager_level_uuid = self.manager_levels.get(manager_level)
            manager_type_uuid = self.manager_types.get(employee["position"])
            responsibility_uuid = self.responsibilities.get('Lederansvar')

            payload = payloads.edit_manager(
                object_uuid=mo_manager['uuid'],
                unit=str(self._generate_uuid(employee['orgUnit'])),
                person=employee_mo_uuid,
                manager_type=manager_type_uuid,
                level=manager_level_uuid,
                responsibility=responsibility_uuid,
                validity=self.validity(employee, edit=True)
            )
            logger.debug('Update manager payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        # TODO: Update Roller

    def terminate_engagement(self, uuid):
        print('Terminating {}'.format(uuid))
        payload = payloads.terminate_engagement(
            uuid, self.latest_date.strftime('%Y-%m-%d')
        )
        logger.debug('Terminate payload: {}'.format(payload))
        response = self.helper._mo_post('details/terminate', payload)
        logger.debug('Terminate response: {}'.format(response.text))
        self._assert(response)

    def start_re_import(self, include_terminations=False):
        """
        Start an opus import, run the oldest available dump that
        has not already been imported.
        """
        dumps = opus_helpers._read_available_dumps()

        run_db = Path(RUN_DB)
        if run_db.is_file():
            xml_date = opus_helpers._next_xml_file(run_db, dumps)

        xml_file = dumps[xml_date]
        self.parser(xml_file)

        for unit in self.units:
            last_changed = datetime.strptime(unit['@lastChanged'], '%Y-%m-%d')
            if last_changed > self.latest_date:
                # This code has been tested for at least 30 seconds and seems to work
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

                # This code has been tested and seems to work.
                engagement = self._find_engagement(employee['@id'], present=True)
                if engagement:
                    self.terminate_engagement(engagement['uuid'])


if __name__ == '__main__':
    diff = OpusImport()
    diff.start_re_import(include_terminations=False)
