import os
import uuid
import logging
import hashlib
import sqlite3
import requests

from pathlib import Path
from requests import Session
from datetime import datetime

from integrations.opus import payloads
from integrations.opus import opus_helpers
from integrations.ad_integration import ad_reader
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.opus.opus_exceptions import EmploymentIdentifierNotUnique

RUN_DB = os.environ.get('RUN_DB')
MOX_BASE = os.environ.get('MOX_BASE')
MORA_BASE = os.environ.get('MORA_BASE')
MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME')


logger = logging.getLogger("opusDiff")


class OpusImport(object):
    def __init__(self, employee_mapping={}):

        self.session = Session()
        self.mox_base = MOX_BASE
        self.org_name = MUNICIPALTY_NAME

        # TODO: Test with an actual employee mapping
        self.employee_forced_uuids = employee_mapping
        self.ad_reader = ad_reader.ADParameterReader()

        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        eng_types = self.helper.read_classes_in_facet('engagement_type')
        self.engagement_types = {}
        for eng_type in eng_types[0]:
            self.engagement_types[eng_type['user_key']] = eng_type['uuid']

        logger.info('Read it systems')
        it_systems = self.helper.read_it_systems()
        for system in it_systems:
            if system['name'] == 'Active Directory':
                self.ad_uuid = system['uuid']  # This could also be a conf-option.

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
        base_hash = hashlib.md5(self.org_name.encode())
        base_digest = base_hash.hexdigest()
        base_uuid = uuid.UUID(base_digest)

        combined_value = (str(base_uuid) + str(value)).encode()
        value_hash = hashlib.md5(combined_value)
        value_digest = value_hash.hexdigest()
        value_uuid = uuid.UUID(value_digest)
        return value_uuid

    # This exists in opus_import. This is the correct position.
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

    def update_unit(self, unit):
        calculated_uuid = self._generate_uuid(unit['@id'])
        mo_unit = self.helper.read_ou(calculated_uuid)
        if mo_unit.get('uuid'):
            print('Existing unit, update')
        else:
            print('Create new unit')

    def update_engagement(self, eng_uuid, employee):
        # It is assumed no new engagement types are added during daily
        # updates. Default 'Ansat' is the default from the initial import
        contract = employee.get('workContractText', 'Ansat')
        eng_type = self.engagement_types[contract]
        unit_uuid = self._generate_uuid(employee['orgUnit'])
        validity = self.validity(employee, edit=True)
        data = {'engagement_type': {'uuid': eng_type},
                'org_unit': {'uuid': str(unit_uuid)},
                'validity': validity}
        payload = payloads.edit_engagement(data, eng_uuid)
        print('Update engagement payload: {}'.format(payload))

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

        print('Create user payload: {}'.format(payload))
        # return_uuid = self.helper._mo_post('e/create', payload).json()
        return_uuid = None
        logger.info('Created employee {} {} with uuid {}'.format(
            employee['firstName'],
            employee['lastName'],
            return_uuid
        ))

        sam_account = ad_info.get('SamAccountName', None)
        if sam_account:
            payload = payloads.connect_it_system_to_user(
                sam_account,
                self.ad_uuid,
                return_uuid
            )
            print('AD account payload: {}'.format(payload))
            logger.info('Added AD account info to {}'.format(cpr))
        return return_uuid

    def update_employee(self, employee):
        cpr = employee['cpr']['#text']
        mo_user = self.helper.read_user(user_cpr=cpr)
        if mo_user is None:
            employee_mo_uuid = self.create_user(employee)
        else:
            employee_mo_uuid = mo_user['uuid']
            # TODO: Here we should update the name of the user

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
                # Create engagement
                pass
            else:
                self.update_engagement(current_mo_eng, employee)

        # TODO: Update manager information

    def terminate_engagement(self, uuid):
        print('Terminating {}'.format(uuid))
        # TODO: Implement this
        pass

    def start_re_import(self):
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
                self.update_unit(unit)

        for employee in self.employees:
            last_changed_str = employee.get('@lastChanged')
            if last_changed_str is not None:
                # This is a true employee-object
                last_changed = datetime.strptime(last_changed_str, '%Y-%m-%d')
                if last_changed > self.latest_date:
                    self.update_employee(employee)
            else:
                # This is a terminated employee, check if engagement is active
                # terminate if it is.
                # Maybe this should be an if, but it really should never happen
                assert(employee['@action'] == 'leave')
                # engagement = self._find_engagement(employee['@id'], present=True)
                # if engagement:
                #     self.terminate_engagement(engagement['uuid'])
                #     pass

            # print(employee)


if __name__ == '__main__':
    diff = OpusImport()
    diff.start_re_import()
