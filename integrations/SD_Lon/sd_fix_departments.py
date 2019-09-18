import os
import logging
import requests
import datetime
import sd_payloads

from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper

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

MORA_BASE = os.environ.get('MORA_BASE', None)


class FixDepartmentsSD(object):
    def __init__(self, from_date):
        logger.info('Start program')
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.from_date = from_date

        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        logger.info('Read org_unit types')
        self.unit_types = self.helper.read_classes_in_facet('org_unit_type')[0]

    def _create_department(self, department, activation_date):
        params = {
            'ActivationDate': activation_date,
            'DeactivationDate': activation_date,
            'DepartmentIdentifier': department['DepartmentIdentifier'],
            'ContactInformationIndicator': 'true',
            'DepartmentNameIndicator': 'true',
            'PostalAddressIndicator': 'false',
            'ProductionUnitIndicator': 'false',
            'UUIDIndicator': 'true',
            'EmploymentDepartmentIndicator': 'false'
        }
        department_info = sd_lookup('GetDepartment20111201', params)

        for unit_type in self.unit_types:
            if unit_type['user_key'] == department['DepartmentLevelIdentifier']:
                unit_type_uuid = unit_type['uuid']

        # TODO: MO currently fails when creating future sub-units
        activation_date = '2019-07-01'  # Temporary!!!!

        payload = sd_payloads.create_org_unit(
            department=department,
            org=self.org_uuid,
            name=department_info['Department']['DepartmentName'],
            unit_type=unit_type_uuid,
            from_date=activation_date
        )
        logger.debug('Create department payload: {}'.format(payload))

        response = self.helper._mo_post('ou/create', payload)
        response.raise_for_status()
        logger.info('Created unit {}'.format(
            department['DepartmentIdentifier'])
        )
        logger.debug('Response: {}'.format(response.text))

    def fix_departments(self):
        params = {
            'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
            'DeactivationDate': '9999-12-31',
            'UUIDIndicator': 'true'
        }
        #TODO: We need to read this without caching!
        organisation = sd_lookup('GetOrganization20111201', params)
        department_lists = organisation['Organization']
        if not isinstance(department_lists, list):
            department_lists = [department_lists]

        total = 0
        checked = 0
        for department_list in department_lists:
            departments = department_list['DepartmentReference']
            total += len(departments)

        logger.info('Checking {} departments'.format(total))
        for department_list in department_lists:
            # All units in this list has same activation date
            activation_date = department_list['ActivationDate']

            departments = department_list['DepartmentReference']
            for department in departments:
                checked += 1
                print('{}/{} {:.2f}%'.format(checked, total, 100.0 * checked/total))
                departments = []
                uuid = department['DepartmentUUIDIdentifier']
                ou = self.helper.read_ou(uuid)
                logger.debug('Check for {}'.format(uuid))

                if 'status' in ou:  # Unit does not exist
                    departments.append(department)
                    logger.info('{} is new in MO'.format(uuid))
                    parent_department = department
                    while 'DepartmentReference' in parent_department:
                        parent_department = parent_department['DepartmentReference']
                        parent_uuid = parent_department['DepartmentUUIDIdentifier']
                        ou = self.helper.read_ou(parent_uuid)
                        logger.debug('Check for {}'.format(parent_uuid))
                        if 'status' in ou:  # Unit does not exist
                            logger.info('{} is new in MO'.format(parent_uuid))
                            departments.append(parent_department)

                # Create actual departments
                while departments:
                    self._create_department(departments.pop(), activation_date)


if __name__ == '__main__':
    from_date = datetime.datetime(2019, 1, 1, 0, 0)

    unit_fixer = FixDepartmentsSD(from_date)
    unit_fixer.fix_departments()
