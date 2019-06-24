import os
import sys
import logging
import sqlite3
import requests
import datetime
import sd_payloads

from pathlib import Path
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper
sys.path.append('../')
import ad_reader

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("sdChangedAt")

# detail_logging = ('AdReader', 'sdCommon', 'sdChangedAt')
detail_logging = ('sdCommon', 'sdChangedAt')
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

MOX_BASE = os.environ.get('MOX_BASE', None)
MORA_BASE = os.environ.get('MORA_BASE', None)
RUN_DB = os.environ.get('RUN_DB', None)

NO_SALLERY = 'status0'
NON_PRIMARY = 'non-primary'
PRIMARY = 'Ansat'


class ChangeAtSD(object):
    def __init__(self, from_date, to_date=None):
        logger.info('Start ChangedAt: From: {}, To: {}'.format(from_date, to_date))
        self.mox_base = MOX_BASE
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.ad_reader = ad_reader.ADParameterReader()
        self.from_date = from_date
        self.to_date = to_date

        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()
        self.employment_response = None

        self.mo_person = None      # Updated continously with the person currently
        self.mo_engagement = None  # being processed.

        logger.info('Read engagement types')
        engagement_types = self.helper.read_classes_in_facet('engagement_type')
        for engagement_type in engagement_types[0]:
            if engagement_type['user_key'] == PRIMARY:
                self.primary = engagement_type['uuid']
            if engagement_type['user_key'] == NON_PRIMARY:
                self.non_primary = engagement_type['uuid']
            if engagement_type['user_key'] == NO_SALLERY:
                self.no_sallery = engagement_type['uuid']

        logger.info('Read it systems')
        it_systems = self.helper.read_it_systems()
        for system in it_systems:
            if system['name'] == 'Active Directory':
                self.ad_uuid = system['uuid']  # This could also be a conf-option.

        logger.info('Read org_unit types')
        self.unit_types = self.helper.read_classes_in_facet('org_unit_type')[0]
        for unit_type in self.unit_types:
            if unit_type['user_key'] == 'Orphan':  # CONF!!!!!
                self.orphan_uuid = unit_type['uuid']

        logger.info('Read job_functions')
        facet_info = self.helper.read_classes_in_facet('engagement_job_function')
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        self.job_functions = {}
        for job in job_functions:
            self.job_functions[job['name']] = job['uuid']

        logger.info('Read leave types')
        facet_info = self.helper.read_classes_in_facet('leave_type')
        self.leave_uuid = facet_info[0][0]['uuid']
        facet_info = self.helper.read_classes_in_facet('association_type')
        self.association_uuid = facet_info[0][0]['uuid']

        # Create non-existent departments
        logger.info('Check for new departments')
        self.check_non_existent_departments()
        self.fix_departments()

    def _add_profession_to_lora(self, profession):
        payload = sd_payloads.profession(profession, self.org_uuid,
                                         self.job_function_facet)
        response = requests.post(
            url=self.mox_base + '/klassifikation/klasse',
            json=payload
        )
        assert response.status_code == 201
        return response.json()

    def _assert(self, response):
        """ Check response is as expected """
        assert response.status_code in (200, 400, 404)
        if response.status_code == 400:
            # Check actual response
            assert response.text.find('not give raise to a new registration') > 0
            logger.debug('Requst had no effect')
        return None

    def read_employment_changed(self):
        if not self.employment_response:  # Caching, we need to get of this
            if self.to_date is not None:
                url = 'GetEmploymentChangedAtDate20111201'
                params = {
                    'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
                    'DeactivationDate': self.to_date.strftime('%d.%m.%Y'),
                    'StatusActiveIndicator': 'true',
                    'DepartmentIndicator': 'true',
                    'EmploymentStatusIndicator': 'true',
                    'ProfessionIndicator': 'true',
                    'WorkingTimeIndicator': 'true',
                    'UUIDIndicator': 'true',
                    'StatusPassiveIndicator': 'true',
                    'SalaryAgreementIndicator': 'false',
                    'SalaryCodeGroupIndicator': 'false'
                }
                response = sd_lookup(url, params=params)
            else:
                url = 'GetEmploymentChanged20111201'
                params = {
                    'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
                    'DeactivationDate': '31.12.9999',
                    'DepartmentIndicator': 'true',
                    'EmploymentStatusIndicator': 'true',
                    'ProfessionIndicator': 'true',
                    'WorkingTimeIndicator': 'true',
                    'UUIDIndicator': 'true',
                    'SalaryAgreementIndicator': 'false',
                    'SalaryCodeGroupIndicator': 'false'
                }
            response = sd_lookup(url, params)
            employment_response = response.get('Person', [])
            if not isinstance(employment_response, list):
                employment_response = [employment_response]

            self.employment_response = employment_response
        return self.employment_response

    def read_person_changed(self):
        if self.to_date is None:
            deactivate_date = '31.12.9999'
        else:
            deactivate_date = self.to_date.strftime('%d.%m.%Y')
        params = {
            'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
            'DeactivationDate': deactivate_date,
            'StatusActiveIndicator': 'true',
            'StatusPassiveIndicator': 'true',
            'ContactInformationIndicator': 'false',
            'PostalAddressIndicator': 'false'
            # TODO: Er der kunder, som vil udlæse adresse-information?
        }
        url = 'GetPersonChangedAtDate20111201'
        response = sd_lookup(url, params=params)
        person_changed = response.get('Person', [])
        if not isinstance(person_changed, list):
            person_changed = [person_changed]
        return person_changed

    def update_changed_persons(self):
        # Så vidt vi ved, består person_changed af navn, cpr nummer og ansættelser.
        # Ansættelser håndteres af update_employment, så vi tjekker for ændringer i
        # navn og opdaterer disse poster. Nye personer oprettes.
        person_changed = self.read_person_changed()
        logger.info('Number of changed persons: {}'.format(len(person_changed)))
        for person in person_changed:
            cpr = person['PersonCivilRegistrationIdentifier']
            logger.debug('Updating: {}'.format(cpr))
            if cpr[-4:] == '0000':
                logger.warning('Skipping fictional user: {}'.format(cpr))
                continue

            # TODO: Shold this go in sd_common?
            given_name = person.get('PersonGivenName', '')
            sur_name = person.get('PersonSurnameName', '')
            sd_name = '{} {}'.format(given_name, sur_name)

            uuid = None
            mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
            ad_info = self.ad_reader.read_user(cpr=cpr)

            if mo_person:
                if mo_person['name'] == sd_name:
                    continue
                uuid = mo_person['uuid']
            else:
                uuid = ad_info.get('ObjectGuid', None)
                logger.debug('{} not in MO or AD, assign random uuid'.format(cpr))
            # Where do we get email and phone from, persumably these informations
            # are not generally available in AD at this point?

            payload = {
                # "givenname": given_name,
                # "surname": sur_name,
                'name': sd_name,
                "cpr_no": cpr,
                "org": {
                    "uuid": self.org_uuid
                }
            }

            if uuid:
                payload['uuid'] = uuid

            return_uuid = self.helper._mo_post('e/create', payload).json()
            logger.info('Created or updated employee {} with uuid {}'.format(
                sd_name,
                return_uuid
            ))

            sam_account = ad_info.get('SamAccountName', None)
            if (not mo_person) and sam_account:
                sd_payloads.connect_it_system_to_user(
                    sam_account,
                    self.ad_uuid,
                    return_uuid
                )
                logger.info('Added AD account info to {}'.format(cpr))

    def check_non_existent_departments(self):
        """
        Runs through all changes and checks if all org units exists in MO.
        If units are missiong they will be created as root units in the
        expectation that they will be moved to the correct place later.
        """
        employments_changed = self.read_employment_changed()
        for employment in employments_changed:
            sd_engagement = employment['Employment']
            if not isinstance(sd_engagement, list):
                sd_engagement = [sd_engagement]
            for engagement in sd_engagement:
                departments = engagement.get('EmploymentDepartment')
                if not departments:
                    continue
                if not isinstance(departments, list):
                    departments = [departments]
                for department in departments:
                    ou = self.helper.read_ou(department['DepartmentUUIDIdentifier'])
                    if 'status' not in ou:  # Unit already exist
                        continue
                    payload = sd_payloads.new_department(
                        department, self.org_uuid, self.orphan_uuid
                    )
                    response = self.helper._mo_post('ou/create', payload)
                    assert response.status_code == 201
                    logger.info('Created unit {}'.format(
                        department['DepartmentIdentifier'])
                    )
        # Consider to return a status that show if we need to re-run organisation.
        return True

    def fix_departments(self):
        params = {
            'ActivationDate': '2019-02-01',
            'DeactivationDate': '9999-12-31',
            'UUIDIndicator': 'true'
        }
        organisation = sd_lookup('GetOrganization20111201', params)
        department_lists = organisation['Organization']
        if not isinstance(department_lists, list):
            department_lists = [department_lists]

        # These will include all unnamed departments
        top_units = self.helper.read_top_units(self.org_uuid)

        for department_list in department_lists:
            departments = department_list['DepartmentReference']
            for department in departments:
                sd_uuid = department['DepartmentUUIDIdentifier']

                current_unit = None
                for unit in top_units:
                    if unit['uuid'] == sd_uuid and unit['name'] == 'Unnamed department':
                        current_unit = unit
                        top_units.remove(unit)
                        break
                if not current_unit:
                    continue
                
                ou_level = department['DepartmentLevelIdentifier']
                unit_uuid = department['DepartmentUUIDIdentifier']
                enhedskode = department['DepartmentIdentifier']
                print(enhedskode)
                for unit_type in self.unit_types:
                    if unit_type['user_key'] == ou_level:
                        unit_type_uuid = unit_type['uuid']

                if 'DepartmentReference' in department:
                    parent_uuid = (department['DepartmentReference']
                                   ['DepartmentUUIDIdentifier'])

                activation_date = department_list['ActivationDate']
                params = {
                    'ActivationDate': activation_date,
                    'DeactivationDate': activation_date,
                    'DepartmentIdentifier': enhedskode,
                    'ContactInformationIndicator': 'true',
                    'DepartmentNameIndicator': 'true',
                    'PostalAddressIndicator': 'false',
                    'ProductionUnitIndicator': 'false',
                    'UUIDIndicator': 'true',
                    'EmploymentDepartmentIndicator': 'false'
                }
                department_info = sd_lookup('GetDepartment20111201', params)
                unit_name = department_info['Department']['DepartmentName']
                payload = sd_payloads.edit_org_unit(
                    unit_uuid=unit_uuid,
                    user_key=enhedskode,
                    name=unit_name,
                    parent=parent_uuid,
                    ou_level=unit_type_uuid,
                    from_date=activation_date
                )
                print(payload)
                print()
                break
        1/0

    
    def _compare_dates(self, first_date, second_date, expected_diff=1):
        """
        Return true if the amount of days between second and first is smaller
        than  expected_diff.
        """
        first = datetime.datetime.strptime(first_date, '%Y-%m-%d')
        second = datetime.datetime.strptime(second_date, '%Y-%m-%d')
        delta = second - first
        # compare = first + datetime.timedelta(days=expected_diff)
        compare = abs(delta.days) <= expected_diff
        logger.debug(
            'Compare. First: {}, second: {}, expected: {}, compare: {}'.format(
                first, second, expected_diff, compare
            )
        )
        return compare

    def _validity(self, engagement_info):
        from_date = engagement_info['ActivationDate']
        to_date = engagement_info['DeactivationDate']
        if to_date == '9999-12-31':
            to_date = None
        validity = {
            'from': from_date,
            'to': to_date
        }
        return validity

    def _find_engagement(self, job_id):
        relevant_engagement = None
        try:
            user_key = str(int(job_id)).zfill(5)
        except ValueError:  # We will end here, if int(job_id) fails
            user_key = job_id

        logger.debug(
            'Find engagement, from date: {}, user_key: {}'.format(
                from_date, user_key
            )
        )

        for mo_eng in self.mo_engagement:
            if mo_eng['user_key'] == user_key:
                relevant_engagement = mo_eng
        return relevant_engagement

    def _update_professions(self, emp_name):
        # Add new profssions to LoRa
        job_uuid = self.job_functions.get(emp_name)
        if job_uuid is None:
            response = self._add_profession_to_lora(emp_name)
            uuid = response['uuid']
            self.job_functions[emp_name] = uuid

    def engagement_components(self, engagement_info):
        job_id = engagement_info['EmploymentIdentifier']

        components = {}
        status_list = engagement_info.get('EmploymentStatus', [])
        if not isinstance(status_list, list):
            status_list = [status_list]
        components['status_list'] = status_list

        professions = engagement_info.get('Profession', [])
        if not isinstance(professions, list):
            professions = [professions]
        components['professions'] = professions

        departments = engagement_info.get('EmploymentDepartment', [])
        if not isinstance(departments, list):
            departments = [departments]
        components['departments'] = departments

        working_time = engagement_info.get('WorkingTime', [])
        if not isinstance(working_time, list):
            working_time = [working_time]
        components['working_time'] = working_time

        # Employment date is not used for anyting
        components['employment_date'] = engagement_info.get('EmploymentDate')
        return job_id, components

    def create_leave(self, status, job_id):
        """ Create a leave for a user """
        logger.info('Create leave, job_id: {}, status: {}'.format(job_id, status))
        # TODO: This code potentially creates duplicated leaves.
        # Implment solution like the one for associations.
        mo_eng = self._find_engagement(job_id)
        payload = sd_payloads.create_leave(mo_eng, self.mo_person, self.leave_uuid,
                                           job_id, self._validity(status))

        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

    def create_association(self, department, person, job_id, validity):
        """ Create a association for a user """
        logger.info('Create association')
        associations = self.helper.read_user_association(person['uuid'],
                                                         read_all=True,
                                                         only_primary=True)
        hit = False
        for association in associations:
            if (
                    association['validity'] == validity and
                    association['org_unit']['uuid'] == department
            ):
                hit = True
        if not hit:
            payload = sd_payloads.create_association(department, person,
                                                     self.association_uuid,
                                                     job_id, validity)
            response = self.helper._mo_post('details/create', payload)
            assert response.status_code == 201

    def apply_NY_logic(self, org_unit, job_id, validity):
        # This must go to sd_common, or some kind of conf
        too_deep = ['Afdelings-niveau', 'NY1-niveau', 'NY2-niveau']

        # Move users and make associations according to NY logic
        ou_info = self.helper.read_ou(org_unit)
        if ou_info['org_unit_type']['name'] in too_deep:
            self.create_association(org_unit, self.mo_person,
                                    job_id, validity)

        while ou_info['org_unit_type']['name'] in too_deep:
            ou_info = ou_info['parent']
        org_unit = ou_info['uuid']
        return org_unit

    def create_new_engagement(self, engagement, status):
        """
        Create a new engagement
        AD integration handled in check for primary engagement.
        """
        job_id, engagement_info = self.engagement_components(engagement)
        validity = self._validity(status)
        also_edit = False
        if (
                len(engagement_info['professions']) > 1 or
                len(engagement_info['working_time']) > 1 or
                len(engagement_info['departments']) > 1
        ):
            also_edit = True

        try:
            org_unit = engagement_info['departments'][0]['DepartmentUUIDIdentifier']
            logger.info('Org unit for new engagement: {}'.format(org_unit))
            org_unit = self.apply_NY_logic(org_unit, job_id, validity)
        except IndexError:
            org_unit = '4f79e266-4080-4300-a800-000006180002'  # CONF!!!!
            logger.error('No unit for engagement {}'.format(job_id))

        try:
            emp_name = engagement_info['professions'][0]['EmploymentName']
        except (KeyError, IndexError):
            emp_name = 'Ukendt'
        self._update_professions(emp_name)

        if status['EmploymentStatusCode'] == '0':
            engagement_type = self.no_sallery
        else:
            engagement_type = self.non_primary

        payload = sd_payloads.create_engagement(org_unit, self.mo_person,
                                                self.job_functions.get(emp_name),
                                                engagement_type, job_id,
                                                engagement_info, validity)

        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

        self.mo_engagement = self.helper.read_user_engagement(
            self.mo_person['uuid'],
            read_all=True,
            only_primary=True,
            use_cache=False
        )
        logger.info('Engagement {} created'.format(job_id))

        if also_edit:
            # This will take of the extra entries
            self.edit_engagement(engagement)

    def _terminate_engagement(self, from_date, job_id):
        mo_engagement = self._find_engagement(job_id)

        if not mo_engagement:
            logger.warning('Terminating non-existing job: {}!'.format(job_id))
            return False

        payload = {
            'type': 'engagement',
            'uuid': mo_engagement['uuid'],
            'validity': {'to': from_date}
        }
        logger.debug('Terminate payload: {}'.format(payload))
        response = self.helper._mo_post('details/terminate', payload)
        logger.debug('Terminate response: {}'.format(response.text))
        self._assert(response)
        return True

    def edit_engagement(self, engagement, validity=None, status0=False):
        """
        Edit an engagement
        """
        job_id, engagement_info = self.engagement_components(engagement)

        mo_engagement = self._find_engagement(job_id)  # DUPLICATE!!!!
        mo_eng = self._find_engagement(job_id)  # DUPLICATE!!!!

        if not validity:
            validity = mo_eng['validity']

        data = {}
        if status0:
            logger.info('Setting {} to status0'.format(job_id))
            data = {'engagement_type': {'uuid': self.no_sallery},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            logger.debug('Status0 payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        for department in engagement_info['departments']:
            logger.info('Change department of engagement {}:'.format(job_id))
            org_unit = department['DepartmentUUIDIdentifier']
            associations = self.helper.read_user_association(self.mo_person['uuid'],
                                                             read_all=True)
            current_association = None
            for association in associations:
                if association['user_key'] == job_id:
                    current_association = association['uuid']
            if current_association:
                logger.debug('We need to move {}'.format(current_association))
                data = {'org_unit': {'uuid': org_unit},
                        'validity': validity}
                payload = sd_payloads.association(data, current_association)
                response = self.helper._mo_post('details/edit', payload)
                self._assert(response)

            org_unit = self.apply_NY_logic(org_unit, job_id, validity)

            logger.debug('Org unit for edited engagement: {}'.format(org_unit))
            data = {'org_unit': {'uuid': org_unit},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        for profession_info in engagement_info['professions']:
            logger.info('Change profession of engagement {}'.format(job_id))
            # We load the name from SD and handles the AD-integration
            # when calculating the primary engagement.
            if 'EmploymentName' in profession_info:
                emp_name = profession_info['EmploymentName']
            else:
                emp_name = profession_info['JobPositionIdentifier']
            logger.debug('Employment name: {}'.format(emp_name))

            self._update_professions(emp_name)
            job_function = self.job_functions.get(emp_name)

            data = {'job_function': {'uuid': job_function},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        for worktime_info in engagement_info['working_time']:
            logger.info('Change working time of engagement {}'.format(job_id))
            working_time = float(worktime_info['OccupationRate'])

            data = {'fraction': int(working_time * 1000000),
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

    def _update_user_employments(self, cpr, sd_engagement):
        for engagement in sd_engagement:
            job_id, eng = self.engagement_components(engagement)
            logger.info('Update Job id: {}'.format(job_id))
            logger.debug('SD Engagement: {}'.format(engagement))

            skip = False
            # If status is present, we have a potential creation
            if eng['status_list']:
                # The EmploymentStatusCode can take a number of magial values
                # that must be handled seperately.
                for status in eng['status_list']:
                    logger.info('Status is: {}'.format(status))
                    code = status['EmploymentStatusCode']

                    if code not in ('0', '1', '3', '7', '8', '9', 'S'):
                        logger.error('Unkown status code {}!'.format(status))
                        1/0

                    if status['EmploymentStatusCode'] == '0':
                        logger.info('Status 0. Cpr: {}, job: {}'.format(cpr, job_id))
                        mo_eng = self._find_engagement(job_id)
                        if mo_eng:
                            logger.info(
                                'Status 0, edit eng {}'.format(mo_eng['uuid'])
                            )
                            self.edit_engagement(engagement, status0=True)
                        else:
                            logger.info('Status 0, create new engagement')
                            self.create_new_engagement(engagement, status)
                        skip = True

                    if status['EmploymentStatusCode'] == '1':
                        logger.info('Setting {} to status 1'.format(job_id))
                        mo_eng = self._find_engagement(job_id)
                        if mo_eng:
                            logger.info(
                                'Status 1, edit eng. {}'.format(mo_eng['uuid'])
                            )
                            validity = self._validity(status)
                            logger.debug('Validity for edit: {}'.format(validity))
                            self.edit_engagement(engagement, validity)
                        else:
                            logger.info('Status 1: Create new engagement')
                            self.create_new_engagement(engagement, status)
                        skip = True

                    if status['EmploymentStatusCode'] == '3':
                        mo_eng = self._find_engagement(job_id)
                        if not mo_eng:
                            logger.info('Leave for non existent eng., create one')
                            self.create_new_engagement(engagement, status)
                        logger.info('Create a leave for {} '.format(cpr))
                        self.create_leave(status, job_id)

                    if status['EmploymentStatusCode'] in ('7', '8'):
                        from_date = status['ActivationDate']
                        logger.info('Terminate {}, job_id {} '.format(cpr, job_id))
                        success = self._terminate_engagement(from_date, job_id)
                        if not success:
                            logger.error('Problem wit job-id: {}'.format(job_id))
                            skip = True

                    if status['EmploymentStatusCode'] in ('S', '9'):
                        skip = True
                        for mo_eng in self.mo_engagement:
                            if not mo_eng['user_key'] == job_id:
                                # User was never actually hired
                                logger.info('Engagement deleted: {}'.format(
                                    status['EmploymentStatusCode']
                                ))
                            else:
                                # Earlier, we checked this for consistency with
                                # existing engagements (see the disaled code below).
                                # However, it seems we should just unconditionally
                                # terminate.
                                end_date = status['ActivationDate']
                                logger.info(
                                    'Status S, 9: Terminate {}'.format(job_id)
                                )
                                self._terminate_engagement(end_date, job_id)
                                """
                                logger.info('Checking consistent end-dates')
                                to_date = mo_eng['validity']['to']
                                if to_date is not None:
                                    consistent = self._compare_dates(
                                        mo_eng['validity']['to'],
                                        status['ActivationDate'],
                                        expected_diff=2
                                    )
                                    logger.info(
                                        'mo: {}, status: {}, consistent: {}'.format(
                                            mo_eng['validity']['to'],
                                            status['ActivationDate'],
                                            consistent
                                        )
                                    )
                                    assert(consistent)
                                else:
                                    end_date = status['ActivationDate']
                                    logger.info(
                                        'Status S, 9: Terminate {}'.format(job_id)
                                    )
                                    self._terminate_engagement(end_date, job_id)
                                """
            if skip:
                continue
            self.edit_engagement(engagement)

    def update_all_employments(self):
        logger.info('Update all employments:')
        employments_changed = self.read_employment_changed()
        logger.info(
            'Update a total of {} employments'.format(
                len(employments_changed)
            )
        )

        i = 0
        for employment in employments_changed:
            print('{}/{}'.format(i, len(employments_changed)))
            i = i + 1

            cpr = employment['PersonCivilRegistrationIdentifier']
            if cpr[-4:] == '0000':
                logger.warning('Skipping fictional user: {}'.format(cpr))
                continue

            logger.info('---------------------')
            logger.info('We are now updating {}'.format(cpr))
            logger.debug('From date: {}'.format(self.from_date))
            logger.debug('To date: {}'.format(self.to_date))
            logger.debug('Employment: {}'.format(employment))

            sd_engagement = employment['Employment']
            if not isinstance(sd_engagement, list):
                sd_engagement = [sd_engagement]

            self.mo_person = self.helper.read_user(user_cpr=cpr,
                                                   org_uuid=self.org_uuid)
            if not self.mo_person:
                for employment_info in sd_engagement:
                    assert (employment_info['EmploymentStatus']
                            ['EmploymentStatusCode']) in ('S', '8')
                logger.warning(
                    'Employment deleted (S) or ended before initial import (8)'
                )
                continue

            self.mo_engagement = self.helper.read_user_engagement(
                self.mo_person['uuid'],
                read_all=True,
                only_primary=True,
                use_cache=False
            )
            self._update_user_employments(cpr, sd_engagement)
            # Re-calculate primary after all updates for user has been performed.
            self.recalculate_primary()

    def _calculate_rate_and_ids(self, mo_engagement):
        max_rate = 0
        min_id = 9999999
        for eng in mo_engagement:
            if 'user_key' not in eng:
                logger.error('Cannot calculate primary!!! Eng: {}'.format(eng))
                return None, None
            employment_id = eng['user_key']

            if not eng['fraction']:
                eng['fraction'] = 0
                continue

            occupation_rate = eng['fraction']
            if eng['fraction'] == max_rate:
                if employment_id < min_id:
                    min_id = employment_id
            if occupation_rate > max_rate:
                max_rate = occupation_rate
                min_id = employment_id
        logger.debug('Min id: {}, Max rate: {}'.format(min_id, max_rate))
        return (min_id, max_rate)

    def _find_cut_dates(self):
        """
        Run throgh entire history of current user and return a list of dates with
        changes in the engagement.
        """
        uuid = self.mo_person['uuid']
        mo_engagement = self.helper.read_user_engagement(
            user=uuid,
            only_primary=True,
            read_all=True,
        )
        dates = set()
        for eng in mo_engagement:
            dates.add(datetime.datetime.strptime(eng['validity']['from'],
                                                 '%Y-%m-%d'))
            if eng['validity']['to']:
                to = datetime.datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
                day_after = to + datetime.timedelta(days=1)
                dates.add(day_after)
            else:
                dates.add(datetime.datetime(9999, 12, 30, 0, 0))

        date_list = sorted(list(dates))
        logger.debug('List of cut-dates: {}'.format(date_list))
        return date_list

    def recalculate_primary(self):
        """
        Re-calculate primary engagement for the enire history of the current user.
        """
        logger.info('Calculate primary engagement')
        date_list = self._find_cut_dates()

        for i in range(0, len(date_list) - 1):
            date = date_list[i]

            mo_engagement = self.helper.read_user_engagement(
                user=self.mo_person['uuid'],
                at=date,
                only_primary=True,  # Do not read extended info from MO.
                use_cache=False
            )
            (min_id, max_rate) = self._calculate_rate_and_ids(mo_engagement)
            if (min_id is None) or (max_rate is None):
                continue

            exactly_one_primary = False
            for eng in mo_engagement:
                if eng['engagement_type']['uuid'] == self.no_sallery:
                    logger.info('Status 0, no update of primary')
                    continue

                if date_list[i + 1] == datetime.datetime(9999, 12, 30, 0, 0):
                    to = None
                else:
                    to = datetime.datetime.strftime(
                        date_list[i + 1] - datetime.timedelta(days=1), "%Y-%m-%d"
                    )
                validity = {
                    'from': datetime.datetime.strftime(date, "%Y-%m-%d"),
                    'to': to
                }

                if 'user_key' not in eng:
                    break
                employment_id = eng['user_key']
                occupation_rate = eng['fraction']

                employment_id = eng['user_key']
                if occupation_rate == max_rate and employment_id == min_id:
                    assert(exactly_one_primary is False)
                    logger.debug('Primary is: {}'.format(employment_id))
                    exactly_one_primary = True
                    data = {
                        'primary': True,
                        'engagement_type': {'uuid': self.primary},
                        'validity': validity
                    }
                    ad_info = self.ad_reader.read_user(cpr=self.mo_person['cpr_no'])
                    logger.debug(
                        'Ad info for {}: {}'.format(
                            self.mo_person['cpr_no'], ad_info
                        )
                    )

                    ad_title = ad_info.get('Title', None)
                    if ad_title:
                        self._update_professions(ad_title)
                        data['job_funcion'] = self.job_functions.get(ad_title)
                else:
                    logger.debug('{} is not primary'.format(employment_id))
                    data = {
                        'primary': False,
                        'engagement_type': {'uuid': self.non_primary},
                        'validity': validity
                    }
                payload = sd_payloads.engagement(data, eng)
                response = self.helper._mo_post('details/edit', payload)
                assert response.status_code in (200, 400)


def _local_db_insert(insert_tuple):
    conn = sqlite3.connect(RUN_DB, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    query = 'insert into runs (from_date, to_date, status) values (?, ?, ?)'
    final_tuple = (
        insert_tuple[0],
        insert_tuple[1],
        insert_tuple[2].format(datetime.datetime.now())
    )
    c.execute(query, final_tuple)
    conn.commit()
    conn.close()


def initialize_changed_at(from_date, run_db, force=False):
    if not run_db.is_file():
        logger.error('Local base not correctly initialized')
        if not force:
            raise Exception('Local base not correctly initialized')
        else:
            logger.info('Force is true, create new db')
            conn = sqlite3.connect(str(run_db))
            c = conn.cursor()
            c.execute("""
              CREATE TABLE runs (id INTEGER PRIMARY KEY,
                from_date timestamp, to_date timestamp, status text)
            """)
            conn.commit()
            conn.close()

    _local_db_insert((from_date, from_date, 'Running since {}'))

    logger.info('Start initial ChangedAt')
    sd_updater = ChangeAtSD(from_date)
    sd_updater.update_changed_persons()
    sd_updater.update_all_employments()
    logger.info('Ended initial ChangedAt')

    _local_db_insert((from_date, from_date, 'Initial import: {}'))


if __name__ == '__main__':
    logger.info('***************')
    logger.info('Program started')
    init = False

    # helper = MoraHelper(hostname=MORA_BASE, use_cache=False) # From __init__
    # unit_types = helper.read_classes_in_facet('org_unit_type')[0]

    """
        ou_level = department['DepartmentLevelIdentifier']
        unit_id = department['DepartmentUUIDIdentifier']
        user_key = department['DepartmentIdentifier']
        parent_uuid = None
        if 'DepartmentReference' in department:
            parent_uuid = (department['DepartmentReference']
                           ['DepartmentUUIDIdentifier'])

        info = self.info[unit_id]
        assert(info['DepartmentLevelIdentifier'] == ou_level)
        logger.debug('Add unit: {}'.format(unit_id))
        if not contains_subunits and parent_uuid is None:
            parent_uuid = 'OrphanUnits'
    """
    if init:
        from_date = datetime.datetime(2019, 6, 2, 0, 0)
        run_db = Path(RUN_DB)
        initialize_changed_at(from_date, run_db, force=True)
        exit()

    conn = sqlite3.connect(RUN_DB, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()

    query = 'select * from runs order by id desc limit 1'
    c.execute(query)
    row = c.fetchone()

    if 'Running' in row[3]:
        print('Critical error')
        logging.error('Previous ChangedAt run did not return!')
        raise Exception('Previous ChangedAt run did not return!')
    else:
        time_diff = datetime.datetime.now() - row[2]
        if time_diff < datetime.timedelta(days=1):
            print('Critical error')
            logging.error('Re-running ChangedAt too early!')
            raise Exception('Re-running ChangedAt too early!')

    # Row[2] contains end_date of last run, this will be the from_date for this run.
    from_date = row[2]
    to_date = from_date + datetime.timedelta(days=1)
    _local_db_insert((from_date, to_date, 'Running since {}'))

    logger.info('Start ChangedAt module')
    sd_updater = ChangeAtSD(from_date, to_date)

    logger.info('Update changed persons')
    sd_updater.update_changed_persons()

    logger.info('Update all emploments')
    sd_updater.update_all_employments()

    _local_db_insert((from_date, to_date, 'Update finished: {}'))
    logger.info('Program stopped.')
