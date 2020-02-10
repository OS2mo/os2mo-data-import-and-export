import json
import logging
import pathlib
import sqlite3
import requests
import datetime
import sd_payloads

from integrations import cpr_mapper
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.ad_integration import ad_reader
from integrations.SD_Lon.sd_common import sd_lookup
# from integrations.SD_Lon.sd_common import generate_uuid
from integrations.SD_Lon.sd_common import mora_assert
from integrations.SD_Lon.sd_common import primary_types
from integrations.SD_Lon.sd_common import calc_employment_id
from integrations.SD_Lon.fix_departments import FixDepartments
from integrations.SD_Lon.calculate_primary import MOPrimaryEngagementUpdater


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("sdChangedAt")

detail_logging = ('sdCommon', 'sdChangedAt', 'updatePrimaryEngagements',
                  'fixDepartments')
for name in logging.root.manager.loggerDict:
    if name in detail_logging:
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)


# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
# TODO: This must be clean up, settings should be loaded by __init__
# and no references should be needed in global scope.
SETTINGS = json.loads(cfg_file.read_text())
RUN_DB = SETTINGS['integrations.SD_Lon.import.run_db']

# TODO: SHOULD WE IMPLEMENT PREDICTABLE ENGAGEMENT UUIDS ALSO IN THIS CODE?!?


class ChangeAtSD(object):
    def __init__(self, from_date, to_date=None):
        self.settings = SETTINGS

        cpr_map = pathlib.Path.cwd() / 'settings' / 'cpr_uuid_map.csv'
        if not cpr_map.is_file():
            logger.error('Did not find cpr mapping')
            raise Exception('Did not find cpr mapping')

        logger.info('Found cpr mapping')
        self.employee_forced_uuids = cpr_mapper.employee_mapper(str(cpr_map))
        self.department_fixer = FixDepartments()
        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)

        use_ad = SETTINGS.get('integrations.SD_Lon.use_ad_integration', True)
        if use_ad:
            logger.info('AD integration in use')
            self.ad_reader = ad_reader.ADParameterReader()
        else:
            logger.info('AD integration not in use')
            self.ad_reader = None

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

        self.primary_types = primary_types(self.helper)

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

        logger.info('Read engagement types')
        # The Opus diff-import contains a slightly more abstrac def to do this
        engagement_types = self.helper.read_classes_in_facet('engagement_type')
        self.engagement_types = {}
        for engagement_type in engagement_types[0]:
            self.engagement_types[engagement_type['user_key']] = engagement_type['uuid']

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

    def read_employment_changed(self):
        if not self.employment_response:  # Caching, we need to get of this
            if self.to_date is not None:
                url = 'GetEmploymentChangedAtDate20111201'
                params = {
                    # 'EmploymentIdentifier': '',
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
                    # 'EmploymentIdentifier': '',
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
        response = sd_lookup(url, params=params)
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

            if self.ad_reader is not None:
                ad_info = self.ad_reader.read_user(cpr=cpr)
            else:
                ad_info = {}

            if mo_person:
                if mo_person['name'] == sd_name:
                    continue
                uuid = mo_person['uuid']
            else:
                uuid = self.employee_forced_uuids.get(cpr)
                logger.info('Employee in force list: {} {}'.format(cpr, uuid))
                if uuid is None and cpr in ad_info:
                    uuid = ad_info[cpr]['ObjectGuid']
                if uuid is None:
                    msg = '{} not in MO, UUID list or AD, assign random uuid'
                    logger.debug(msg.format(cpr))

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
                payload = sd_payloads.connect_it_system_to_user(
                    sam_account,
                    self.ad_uuid,
                    return_uuid
                )
                logger.debug('Connect it-system: {}'.format(payload))
                response = self.helper._mo_post('details/create', payload)
                assert response.status_code == 201
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

    def _validity(self, engagement_info, original_end=None, cut=False):
        """
        Extract a validity object from the supplied SD information.
        If the validity extends outside the current engagment, the
        change is either refused (by returning None) or cut to the
        length of the current engagment.
        :param engagement_info: The SD object to extract from.
        :param orginal_end: The engagment end to compare with.
        :param cut: If True the returned validity will cut to fit
        rather than rejeted, if the validity is too long.
        :return: A validity dict suitable for a MO payload. None if
        the change is rejected.
        """
        from_date = engagement_info['ActivationDate']
        to_date = engagement_info['DeactivationDate']

        if original_end is not None:
            edit_from = datetime.datetime.strptime(from_date, '%Y-%m-%d')
            edit_end = datetime.datetime.strptime(to_date, '%Y-%m-%d')
            eng_end = datetime.datetime.strptime(original_end, '%Y-%m-%d')
            if edit_from >= eng_end:
                logger.info('This edit starts after the end of the engagement')
                return None

            if (edit_end > eng_end):
                if cut:
                    to_date = datetime.datetime.strftime(eng_end, '%Y-%m-%d')
                else:
                    logger.info('This edit would have extended outside engagement')
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
                self.from_date, user_key
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

    # Possibly this should be generalized to also be able to add engagement_types
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

        # Notice, the expected and desired behaviour for leaves is for the engagement
        # to continue during the leave. It turns out this is actually what happens
        # because a leave is apparently always accompanied by a worktime-update that
        # forces an edit to the engagement that will extend it to span the
        # leave. If this ever turns out not to hold, add a dummy-edit to the
        # engagement here.
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
        logger.debug('Associations read from MO: {}'.format(associations))
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
        msg = 'Apply NY logic for job: {}, unit: {}, validity: {}'
        logger.debug(msg.format(job_id, org_unit, validity))
        too_deep = self.settings['integrations.SD_Lon.import.too_deep']
        # Move users and make associations according to NY logic
        today = datetime.datetime.today()
        ou_info = self.helper.read_ou(org_unit, use_cache=False)
        if 'status' in ou_info:
            # This unit does not exist, read its state in the not-too
            # distant future.
            fix_date = today + datetime.timedelta(weeks=80)
            self.department_fixer.fix_or_create_branch(org_unit, fix_date)
            ou_info = self.helper.read_ou(org_unit, use_cache=False)

        if ou_info['org_unit_level']['user_key'] in too_deep:
            self.create_association(org_unit, self.mo_person,
                                    job_id, validity)

        # logger.debug('OU info is currently: {}'.format(ou_info))
        while ou_info['org_unit_level']['user_key'] in too_deep:
            ou_info = ou_info['parent']
            logger.debug('Parent unit: {}'.format(ou_info))
        org_unit = ou_info['uuid']

        return org_unit

    def create_new_engagement(self, engagement, status):
        """
        Create a new engagement
        AD integration handled in check for primary engagement.
        """
        user_key, engagement_info = self.engagement_components(engagement)
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
            org_unit = self.apply_NY_logic(org_unit, user_key, validity)
        except IndexError:
            # This can be removed if we do not see the exception:
            # org_unit = '4f79e266-4080-4300-a800-000006180002'  # CONF!!!!
            msg = 'No unit for engagement {}'.format(user_key)
            logger.error(msg)
            raise Exception(msg)

        # JobPositionIdentifier is supposedly always returned
        job_position = engagement_info['professions'][0]['JobPositionIdentifier']

        try:
            emp_name = engagement_info['professions'][0]['EmploymentName']
        except (KeyError, IndexError):
            emp_name = 'Ukendt'
        self._update_professions(emp_name)

        if status['EmploymentStatusCode'] == '0':
            primary = self.primary_types['no_salary']
        else:
            primary = self.primary_types['non_primary']

        split = self.settings['integrations.SD_Lon.monthly_hourly_divide']
        employment_id = calc_employment_id(engagement)
        if employment_id['value'] < split:
            engagement_type = self.engagement_types.get('månedsløn')
        elif (split - 1) < employment_id['value'] < 999999:
            engagement_type = self.engagement_types.get('timeløn')
        else:  # This happens if EmploymentID is not a number
            # Will fail if a new job position emerges
            engagement_type = self.engagement_types.get(job_position)
            logger.info('Non-nummeric id. Job pos id: {}'.format(job_position))

        payload = sd_payloads.create_engagement(
            org_unit=org_unit,
            mo_person=self.mo_person,
            job_function=self.job_functions.get(emp_name),
            engagement_type=engagement_type,
            primary=primary,
            user_key=user_key,
            engagement_info=engagement_info,
            validity=validity
        )

        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

        self.mo_engagement = self.helper.read_user_engagement(
            self.mo_person['uuid'],
            read_all=True,
            only_primary=True,
            use_cache=False
        )
        logger.info('Engagement {} created'.format(user_key))

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
        mora_assert(response)
        return True

    def _edit_engagement_department(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
        for department in engagement_info['departments']:
            logger.info('Change department of engagement {}:'.format(job_id))
            logger.debug('Department object: {}'.format(department))

            validity = self._validity(department,
                                      mo_eng['validity']['to'],
                                      cut=True)
            if validity is None:
                continue

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
                mora_assert(response)

            org_unit = self.apply_NY_logic(org_unit, job_id, validity)

            logger.debug('New org unit for edited engagement: {}'.format(org_unit))
            data = {'org_unit': {'uuid': org_unit},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
            response = self.helper._mo_post('details/edit', payload)
            mora_assert(response)

    def _edit_engagement_profession(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
        for profession_info in engagement_info['professions']:
            logger.info('Change profession of engagement {}'.format(job_id))
            # We load the name from SD and handles the AD-integration
            # when calculating the primary engagement.
            if 'EmploymentName' in profession_info:
                emp_name = profession_info['EmploymentName']
            else:
                emp_name = profession_info['JobPositionIdentifier']
            logger.debug('Employment name: {}'.format(emp_name))
            validity = self._validity(profession_info, mo_eng['validity']['to'],
                                      cut=True)
            if validity is None:
                continue

            self._update_professions(emp_name)
            job_function = self.job_functions.get(emp_name)

            data = {'job_function': {'uuid': job_function},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug('Update profession payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            mora_assert(response)

    def _edit_engagement_worktime(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
        for worktime_info in engagement_info['working_time']:
            logger.info('Change working time of engagement {}'.format(job_id))

            # Should this have an end comparison and cut=True?
            # Most likely not, but be aware of the option.
            validity = self._validity(worktime_info, mo_eng['validity']['to'])
            # As far as we know, this can only happen for work time
            if validity is None:
                continue

            working_time = float(worktime_info['OccupationRate'])
            data = {'fraction': int(working_time * 1000000),
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug('Change worktime, payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            mora_assert(response)

    def edit_engagement(self, engagement, validity=None, status0=False):
        """
        Edit an engagement
        """
        job_id, engagement_info = self.engagement_components(engagement)

        mo_eng = self._find_engagement(job_id)
        if not mo_eng:
            logger.error('Engagement {} has never existed!'.format(job_id))
            return

        if not validity:
            validity = mo_eng['validity']

        data = {}
        if status0:
            logger.info('Setting {} to status0'.format(job_id))
            data = {'primary_type': {'uuid': self.primary_types['non_primary']},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug('Status0 payload: {}'.format(payload))
            response = self.helper._mo_post('details/edit', payload)
            mora_assert(response)

        self._edit_engagement_department(engagement, mo_eng)
        self._edit_engagement_profession(engagement, mo_eng)
        self._edit_engagement_worktime(engagement, mo_eng)

    def _handle_status_chages(self, cpr, engagement):
        skip = False
        # The EmploymentStatusCode can take a number of magical values.
        # that must be handled seperately.
        job_id, eng = self.engagement_components(engagement)
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
                    logger.info('Status 0, edit eng {}'.format(mo_eng['uuid']))
                    self.edit_engagement(engagement, status0=True)
                else:
                    logger.info('Status 0, create new engagement')
                    self.create_new_engagement(engagement, status)
                skip = True

            if status['EmploymentStatusCode'] == '1':
                logger.info('Setting {} to status 1'.format(job_id))
                mo_eng = self._find_engagement(job_id)
                if mo_eng:
                    logger.info('Status 1, edit eng. {}'.format(mo_eng['uuid']))

                    validity = self._validity(status)
                    logger.debug('Validity for edit: {}'.format(validity))
                    data = {
                        'validity': validity,
                        'primary_type': {'uuid': self.primary_types['non_primary']},
                    }
                    payload = sd_payloads.engagement(data, mo_eng)
                    logger.debug('Edit status 1, payload: {}'.format(payload))
                    response = self.helper._mo_post('details/edit', payload)
                    mora_assert(response)
                    self.mo_engagement = self.helper.read_user_engagement(
                        self.mo_person['uuid'], read_all=True,
                        only_primary=True, use_cache=False
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
                    if mo_eng['user_key'] == job_id:
                        logger.info('Status S, 9: Terminate {}'.format(job_id))
                        self._terminate_engagement(status['ActivationDate'], job_id)
        return skip

    def _update_user_employments(self, cpr, sd_engagement):
        for engagement in sd_engagement:
            job_id, eng = self.engagement_components(engagement)
            logger.info('Update Job id: {}'.format(job_id))
            logger.debug('SD Engagement: {}'.format(engagement))
            skip = False
            # If status is present, we have a potential creation
            if eng['status_list']:
                skip = self._handle_status_chages(cpr, engagement)
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
                    emp_status = employment_info['EmploymentStatus']
                    if isinstance(emp_status, list):
                        code = emp_status[0]['EmploymentStatusCode']
                    else:
                        code = emp_status['EmploymentStatusCode']
                    if code in ('S', '7', '8'):
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
    conn = sqlite3.connect(SETTINGS['integrations.SD_Lon.import.run_db'],
                           detect_types=sqlite3.PARSE_DECLTYPES)
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
    logging.basicConfig(
        format='%(levelname)s %(asctime)s %(name)s %(message)s',
        level=LOG_LEVEL,
        filename=LOG_FILE
    )
    logger.info('***************')
    logger.info('Program started')
    init = False

    from_date = datetime.datetime.strptime(
        SETTINGS['integrations.SD_Lon.global_from_date'],
        '%Y-%m-%d'
    )

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
