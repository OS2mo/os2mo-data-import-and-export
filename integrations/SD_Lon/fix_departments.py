import json
import pathlib
import logging
import requests
import datetime
import sd_payloads

from integrations.SD_Lon.sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.SD_Lon.exceptions import ValdityTooEarlyException

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'fix_sd_departments.log'

logger = logging.getLogger("sdFixDepartments")

detail_logging = ('sdCommon', 'sdFixDepartments')
for name in logging.root.manager.loggerDict:
    if name in detail_logging:
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class FixDepartmentsSD(object):
    def __init__(self, from_date):
        logger.info('Start program')
        # TODO: Soon we have done this 4 times. Should we make a small settings
        # importer, that will also handle datatype for specicic keys?
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.institution_uuid = self.get_institution()
        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        self.from_date = from_date

        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        logger.info('Read org_unit types')
        self.unit_types = self.helper.read_classes_in_facet('org_unit_type')[0]

    # def create_department(self, department, activation_date):
    #     params = {
    #         'ActivationDate': activation_date,
    #         'DeactivationDate': activation_date,
    #         'DepartmentIdentifier': department['DepartmentIdentifier'],
    #         'ContactInformationIndicator': 'true',
    #         'DepartmentNameIndicator': 'true',
    #         'PostalAddressIndicator': 'false',
    #         'ProductionUnitIndicator': 'false',
    #         'UUIDIndicator': 'true',
    #         'EmploymentDepartmentIndicator': 'false'
    #     }
    #     department_info = sd_lookup('GetDepartment20111201', params)

    #     for unit_type in self.unit_types:
    #         if unit_type['user_key'] == department['DepartmentLevelIdentifier']:
    #             unit_type_uuid = unit_type['uuid']

    #     payload = sd_payloads.create_org_unit(
    #         department=department,
    #         org=self.org_uuid,
    #         name=department_info['Department']['DepartmentName'],
    #         unit_type=unit_type_uuid,
    #         from_date=department_info['Department']['ActivationDate']
    #     )
    #     logger.debug('Create department payload: {}'.format(payload))

    #     response = self.helper._mo_post('ou/create', payload)
    #     response.raise_for_status()
    #     logger.info('Created unit {}'.format(
    #         department['DepartmentIdentifier'])
    #     )
    #     logger.debug('Response: {}'.format(response.text))

    # def fix_departments(self):
    #     params = {
    #         'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
    #         'DeactivationDate': '31.12.9999',
    #         'UUIDIndicator': 'true'
    #     }
    #     # TODO: We need to read this without caching!
    #     organisation = sd_lookup('GetOrganization20111201', params)
    #     department_lists = organisation['Organization']
    #     if not isinstance(department_lists, list):
    #         department_lists = [department_lists]

    #     total = 0
    #     checked = 0
    #     for department_list in department_lists:
    #         departments = department_list['DepartmentReference']
    #         total += len(departments)

    #     logger.info('Checking {} departments'.format(total))
    #     for department_list in department_lists:
    #         # All units in this list has same activation date
    #         activation_date = department_list['ActivationDate']

    #         departments = department_list['DepartmentReference']
    #         for department in departments:
    #             checked += 1
    #            print('{}/{} {:.2f}%'.format(checked, total, 100.0 * checked/total))
    #             departments = []
    #             uuid = department['DepartmentUUIDIdentifier']
    #             ou = self.helper.read_ou(uuid)
    #             logger.debug('Check for {}'.format(uuid))

    #             if 'status' in ou:  # Unit does not exist
    #                 print('klaf')
    #                 departments.append(department)
    #                 logger.info('{} is new in MO'.format(uuid))
    #                 parent_department = department
    #                 while 'DepartmentReference' in parent_department:
    #                    parent_department = parent_department['DepartmentReference']
    #                     parent_uuid = parent_department['DepartmentUUIDIdentifier']
    #                     ou = self.helper.read_ou(parent_uuid)
    #                     logger.debug('Check for {}'.format(parent_uuid))
    #                     if 'status' in ou:  # Unit does not exist
    #                         logger.info('{} is new in MO'.format(parent_uuid))
    #                         departments.append(parent_department)

    #             # Create actual departments
    #             while departments:
    #                 self.create_department(departments.pop(), activation_date)

    def get_institution(self):
        inst_id = self.settings['integrations.SD_Lon.institution_identifier']
        params = {
            'UUIDIndicator': 'true',
            'InstitutionIdentifier': inst_id
        }
        institution_info = sd_lookup('GetInstitution20111201', params)
        # print(institution_info.keys())
        institution = institution_info['Region']['Institution']
        institution_uuid = institution['InstitutionUUIDIdentifier']
        return institution_uuid

    def create_single_department(self, shortname, date):
        """ Create a single department at a single snapshot in time """
        activation_date = date.strftime('%d.%m.%Y')
        params = {
            'ActivationDate': activation_date,
            'DeactivationDate': activation_date,
            'DepartmentIdentifier': shortname,
            'ContactInformationIndicator': 'true',
            'DepartmentNameIndicator': 'true',
            'PostalAddressIndicator': 'false',
            'ProductionUnitIndicator': 'false',
            'UUIDIndicator': 'true',
            'EmploymentDepartmentIndicator': 'false'
        }
        department_info = sd_lookup('GetDepartment20111201', params)
        department = department_info['Department']
        parent = self.get_parent(department['DepartmentUUIDIdentifier'], date)

        for unit_type in self.unit_types:
            if unit_type['user_key'] == department['DepartmentLevelIdentifier']:
                unit_type_uuid = unit_type['uuid']

        payload = sd_payloads.create_single_org_unit(
            department=department,
            unit_type=unit_type_uuid,
            parent=parent
        )
        logger.debug('Create department payload: {}'.format(payload))
        response = self.helper._mo_post('ou/create', payload)
        response.raise_for_status()
        logger.info('Created unit {}'.format(
            department['DepartmentIdentifier'])
        )
        logger.debug('Response: {}'.format(response.text))

    def fix_specific_department(self, shortname):
        """
        Run through historic information from SD and attempt to replicate
        this in MO. Departments will not be created, so the existence in MO
        mus be confirmed by other means.
        """
        # Semi-arbitrary start date for historic import
        from_date = datetime.datetime(2000, 1, 1, 0, 0)
        logger.info('Fix import of department {}'.format(shortname))

        params = {
            'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
            'DeactivationDate': '9999-12-31',
            'DepartmentIdentifier': shortname,
            'UUIDIndicator': 'true',
            'DepartmentNameIndicator': 'true'
        }

        department = sd_lookup('GetDepartment20111201', params, use_cache=False)
        validities = department['Department']
        if isinstance(validities, dict):
            validities = [validities]

        for validity in validities:
            assert shortname == validity['DepartmentIdentifier']
            validity_date = datetime.datetime.strptime(validity['ActivationDate'],
                                                       '%Y-%m-%d')
            user_key = shortname
            name = validity['DepartmentName']
            unit_uuid = validity['DepartmentUUIDIdentifier']

            for unit_type in self.unit_types:
                if unit_type['user_key'] == validity['DepartmentLevelIdentifier']:
                    unit_type_uuid = unit_type['uuid']

            from_date = validity_date.strftime('%Y-%m-%d')

            # SD has a challenge with the internal validity-consistency
            try:
                parent = self.get_parent(unit_uuid, validity_date)
                logger.info('Unit parent at {} is {}'.format(from_date, parent))
            except ValdityTooEarlyException:
                logger.error('Could not get unit at correct validity, use today')
                parent = self.get_parent(unit_uuid, datetime.datetime.now())

            payload = sd_payloads.edit_org_unit(user_key, name, unit_uuid, parent,
                                                unit_type_uuid, from_date)
            logger.debug('Edit payload to fix unit: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            if response.status_code == 400:
                assert(response.text.find('raise to a new registration') > 0)
            else:
                response.raise_for_status()
            logger.debug('Response: {}'.format(response.text))

    def get_department(self, validity, shortname=None, uuid=None):
        params = {
            'ActivationDate': validity['from_date'],
            'DeactivationDate': validity['to_date'],
            'ContactInformationIndicator': 'true',
            'DepartmentNameIndicator': 'true',
            'PostalAddressIndicator': 'false',
            'ProductionUnitIndicator': 'false',
            'UUIDIndicator': 'true',
            'EmploymentDepartmentIndicator': 'false'
        }
        if uuid is not None:
            params['DepartmentUUIDIdentifier'] = uuid
        if shortname is not None:
            params['DepartmentIdentifier'] = shortname

        if uuid is None and shortname is None:
            raise Exception('Provide either uuid or shortname')

        department_info = sd_lookup('GetDepartment20111201', params)
        department = department_info['Department']
        return department

    def get_parent(self, unit_uuid, validity_date):
        params = {
            'EffectiveDate': validity_date.strftime('%d.%m.%Y'),
            'DepartmentUUIDIdentifier': unit_uuid
        }
        parent_response = sd_lookup('GetDepartmentParent20190701', params)
        if 'DepartmentParent' not in parent_response:
            logger.error('No parent found at validity: {}'.format(validity_date))
            raise ValdityTooEarlyException()
        parent = parent_response['DepartmentParent']['DepartmentUUIDIdentifier']
        if parent == self.institution_uuid:
            parent = None
        return parent

    def get_all_parents(self, shortname, validity_date):
        validity = {
            'from_date': validity_date.strftime('%d.%m.%Y'),
            'to_date': validity_date.strftime('%d.%m.%Y')
        }

        deparment_branch = []
        department = self.get_department(validity=validity, shortname=shortname)
        deparment_branch.append(department['DepartmentUUIDIdentifier'])
        current_uuid = self.get_parent(department['DepartmentUUIDIdentifier'],
                                       validity_date=validity_date)

        while current_uuid is not None:
            department = self.get_department(validity=validity, uuid=current_uuid)
            shortname = department['DepartmentIdentifier']
            level = department['DepartmentLevelIdentifier']
            uuid = department['DepartmentUUIDIdentifier']
            current_uuid = self.get_parent(current_uuid, validity_date=validity_date)
            msg = 'Department: {}, uuid: {}, level: {}'
            logger.debug(msg.format(shortname, uuid, level))


if __name__ == '__main__':
    # from_date = datetime.datetime(2019, 1, 1, 0, 0)
    from_date = datetime.datetime(1900, 1, 1, 0, 0)

    unit_fixer = FixDepartmentsSD(from_date)
    # print(unit_fixer.fix_specific_department('SUFB'))

    # print(unit_fixer.get_parent('1fe1b85a-66c3-4100-be00-000001540001',
    #                             datetime.datetime.now()))
    print(unit_fixer.get_all_parents('SUFB', from_date))

    # from_date = datetime.datetime(2019, 10, 1, 0, 0)
    # unit_fixer.create_single_department('5SE', from_date)

    # unit_fixer.fix_specific_department('0P84')
    # unit_fixer.fix_specific_department('4OS')
    # unit_fixer.fix_specific_department('6JI')
    # unit_fixer.fix_specific_department('8AR')
    # unit_fixer.fix_specific_department('9ØP')
    # unit_fixer.fix_specific_department('10V')

    unit_fixer.fix_departments()

    # params = {
    #     'ActivationDate': '01.08.2019',
    #     'DeactivationDate': '01.08.2019',
    #     'DepartmentIdentifier': '5SE',
    #     'UUIDIndicator': 'true',
    #     'DepartmentNameIndicator': 'true'
    # }
    # department = sd_lookup('GetDepartment20111201', params)
    # unit_fixer.create_department(department['Department'], '01.08.2019')
