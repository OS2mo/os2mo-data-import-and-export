import os
import json
import logging
import pathlib
import sqlite3
import requests
import datetime
import sd_common
import sd_payloads

from integrations.SD_Lon.calculate_primary import MOPrimaryEngagementUpdater
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.ad_integration import ad_reader


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("sdChangedAt")

detail_logging = ('sdCommon', 'sdChangedAt', 'updatePrimaryEngagements')
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

RUN_DB = os.environ.get('RUN_DB', None)
SETTINGS_FILE = os.environ.get('SETTINGS_FILE')


class ChangeAtSD(object):
    def __init__(self, from_date, to_date=None):
        logger.info('Start ChangedAt: From: {}, To: {}'.format(from_date, to_date))
        cfg_file = pathlib.Path.cwd() / 'settings' / SETTINGS_FILE
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        self.ad_reader = ad_reader.ADParameterReader()
        self.updater = MOPrimaryEngagementUpdater()
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

        self.eng_types = sd_common.engagement_types(self.helper)

        logger.info('Read it systems')
        it_systems = self.helper.read_it_systems()
        for system in it_systems:
            if system['name'] == 'Active Directory':
                self.ad_uuid = system['uuid']  # This could also be a conf-option.

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

    def _add_profession_to_lora(self, profession):
        payload = sd_payloads.profession(profession, self.org_uuid,
                                         self.job_function_facet)
        response = requests.post(
            url=self.settings['mox.base'] + '/klassifikation/klasse',
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
                    # 'EmploymentIdentifier': '', # DELETE!!!
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
                response = sd_common.sd_lookup(url, params=params)
            else:
                url = 'GetEmploymentChanged20111201'
                params = {
                    # 'EmploymentIdentifier': '', # DELETE!!!
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
            response = sd_common.sd_lookup(url, params)

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
        response = sd_common.sd_lookup(url, params=params)
        person_changed = response.get('Person', [])
        if not isinstance(person_changed, list):
            person_changed = [person_changed]
        return person_changed

    def read_person(self, cpr):
        params = {
            'EffectiveDate': self.from_date.strftime('%d.%m.%Y'),
            'PersonCivilRegistrationIdentifier': cpr,
            'StatusActiveIndicator': 'True',
            'StatusPassiveIndicator': 'false',
            'ContactInformationIndicator': 'false',
            'PostalAddressIndicator': 'false'
        }
        url = 'GetPerson20111201'
        response = sd_common.sd_lookup(url, params=params)
        person = response.get('Person', [])

        if not isinstance(person, list):
            person = [person]
        return person

    def update_changed_persons(self, cpr=None):
        # Ansættelser håndteres af update_employment, så vi tjekker for ændringer i
        # navn og opdaterer disse poster. Nye personer oprettes.
        if cpr is not None:
            person_changed = self.read_person(cpr)
        else:
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
                'givenname': given_name,
                'surname': sur_name,
                'cpr_no': cpr,
                'org': {
                    'uuid': self.org_uuid
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

    def _validity(self, engagement_info, original_end=None):
        from_date = engagement_info['ActivationDate']
        to_date = engagement_info['DeactivationDate']

        if original_end is not None:
            edit_end = datetime.datetime.strptime(to_date, '%Y-%m-%d')
            eng_end = datetime.datetime.strptime(original_end, '%Y-%m-%d')
            if (edit_end > eng_end):
                logger.warning('This edit would have extended outside engagement')
                return None

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

        if relevant_engagement is None:
            msg = 'Fruitlessly searched for {} in {}'.format(job_id,
                                                             self.mo_engagement)
            logger.info(msg)
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
        logger.info('Consider to create an association')
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
            logger.info('Association needs to be created')
            payload = sd_payloads.create_association(department, person,
                                                     self.association_uuid,
                                                     job_id, validity)
            response = self.helper._mo_post('details/create', payload)
            assert response.status_code == 201
        else:
            logger.info('No new Association is needed')

    def apply_NY_logic(self, org_unit, job_id, validity):
        # This must go to sd_common, or some kind of conf
        too_deep = self.settings['integrations.SD_Lon.import.too_deep']
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
        logger.debug('Create new engagement: also_edit: {}'.format(also_edit))

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
            engagement_type = self.eng_types['no_salary']
        else:
            engagement_type = self.eng_types['non_primary']

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

        # In MO, the termination date is the last day of work,
        # in SD it is the first day of non-work.
        date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
        terminate_datetime = date - datetime.timedelta(days=1)
        terminate_date = terminate_datetime.strftime('%Y-%m-%d')

        payload = {
            'type': 'engagement',
            'uuid': mo_engagement['uuid'],
            'validity': {'to': terminate_date}
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

        mo_eng = self._find_engagement(job_id)

        if not validity:
            validity = mo_eng['validity']

        data = {}
        if status0:
            logger.info('Setting {} to status0'.format(job_id))
            data = {'engagement_type': {'uuid': self.eng_types['non_primary']},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug('Status0 payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        for department in engagement_info['departments']:
            logger.info('Change department of engagement {}:'.format(job_id))
            logger.debug('Department object: {}'.format(department))

            # This line most likely gave us a lot of bugs...
            # validity = self._validity(department)

            logger.debug('Validity of this department change: {}'.format(validity))
            org_unit = department['DepartmentUUIDIdentifier']
            associations = self.helper.read_user_association(self.mo_person['uuid'],
                                                             read_all=True)
            logger.debug('User associations: {}'.format(associations))
            current_association = None
            for association in associations:
                if association['user_key'] == job_id:
                    current_association = association['uuid']

            if current_association:
                logger.debug('We need to move {}'.format(current_association))
                data = {'org_unit': {'uuid': org_unit},
                        'validity': validity}
                payload = sd_payloads.association(data, current_association)
                logger.debug('Association edit payload: {}'.format(payload))
                response = self.helper._mo_post('details/edit', payload)
                self._assert(response)

            org_unit = self.apply_NY_logic(org_unit, job_id, validity)

            logger.debug('New org unit for edited engagement: {}'.format(org_unit))
            data = {'org_unit': {'uuid': org_unit},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
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
            validity = self._validity(profession_info)

            self._update_professions(emp_name)
            job_function = self.job_functions.get(emp_name)

            data = {'job_function': {'uuid': job_function},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug('Update profession payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        for worktime_info in engagement_info['working_time']:
            logger.info('Change working time of engagement {}'.format(job_id))
            validity = self._validity(worktime_info, mo_eng['validity']['to'])
            # As far as we know, this can only happen for work time
            if validity is None:
                continue

            working_time = float(worktime_info['OccupationRate'])

            data = {'fraction': int(working_time * 1000000),
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
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
                # The EmploymentStatusCode can take a number of magical values.
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
                            data = {
                                'validity': validity,
                                'engagement_type': {'uuid':
                                                    self.eng_types['non_primary']},
                            }
                            payload = sd_payloads.engagement(data, mo_eng)
                            response = self.helper._mo_post('details/edit', payload)
                            self._assert(response)
                            self.mo_engagement = self.helper.read_user_engagement(
                                self.mo_person['uuid'],
                                read_all=True,
                                only_primary=True,
                                use_cache=False
                            )

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
            self.updater.set_current_person(mo_person=self.mo_person)

            if not self.mo_person:
                for employment_info in sd_engagement:
                    if (
                            (employment_info['EmploymentStatus']
                             ['EmploymentStatusCode'])
                            in ('S', '7', '8')
                    ):
                        logger.warning(
                            'Employment deleted or ended before initial import.'
                        )
                    else:
                        logger.warning('This person should be in MO, but is not')
                        self.update_changed_persons(cpr=cpr)
                        self.mo_person = self.helper.read_user(
                            user_cpr=cpr,
                            org_uuid=self.org_uuid
                        )
                        self.updater.set_current_person(mo_person=self.mo_person)

            if self.mo_person:
                self.mo_engagement = self.helper.read_user_engagement(
                    self.mo_person['uuid'],
                    read_all=True,
                    only_primary=True,
                    use_cache=False
                )
                self._update_user_employments(cpr, sd_engagement)
                self.updater.set_current_person(uuid=self.mo_person['uuid'])

                # Re-calculate primary after all updates for user has been performed.
                self.updater.recalculate_primary()


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
    sd_updater.update_changed_persons() # REENABLE!!!!!
    sd_updater.update_all_employments()
    logger.info('Ended initial ChangedAt')

    _local_db_insert((from_date, from_date, 'Initial import: {}'))


if __name__ == '__main__':
    logger.info('***************')
    logger.info('Program started')
    init = False
    from_date = datetime.datetime(2019, 10, 1, 0, 0)

    if init:
        run_db = pathlib.Path(RUN_DB)
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

    # from_date = datetime.datetime(2019, 9, 26, 0, 0)
    # to_date = datetime.datetime(2019, 9, 27, 0, 0)
    # sd_updater = ChangeAtSD(from_date, to_date)

    # cpr = ''
    # sd_updater.mo_person = sd_updater.helper.read_user(user_cpr=cpr,
    #                                                    org_uuid=sd_updater.org_uuid)
    # sd_updater.recalculate_primary()
