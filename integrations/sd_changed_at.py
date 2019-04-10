import os
import requests
import datetime
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper
MOX_BASE = os.environ.get('MOX_BASE', None)


class ChangeAtSD(object):

    def __init__(self, from_date, to_date):
        self.mox_base = MOX_BASE
        self.helper = MoraHelper(use_cache=False)
        self.from_date = from_date
        self.to_date = to_date
        self.org_uuid = self.helper.read_organisation()

        self.employment_response = None

        self.mo_person = None      # Updated continously with the person currently
        self.mo_engagement = None  # being processed.

        facet_info = self.helper.read_classes_in_facet('engagement_job_function')
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        self.job_functions = {}
        for job in job_functions:
            self.job_functions[job['name']] = job['uuid']

        # If this assertment fails, we will need to re-run the organisation
        # stucture through the normal importer.
        assert self.check_non_existent_departments()

    def _add_profession_to_lora(self, profession):
        validity = {
            'from': '1900-01-01',
            'to': 'infinity'
        }

        # "integrationsdata":  # TODO: Check this
        properties = {
            'brugervendtnoegle': profession,
            'titel':  profession,
            'omfang': 'TEXT',
            "virkning": validity
        }
        attributter = {
            'klasseegenskaber': [properties]
        }
        relationer = {
            'ansvarlig': [
                {
                    'objekttype': 'organisation',
                    'uuid': self.org_uuid,
                    'virkning': validity
                }
            ],
            'facet': [
                {
                    'objekttype': 'facet',
                    'uuid': self.job_function_facet,
                    'virkning': validity
                }
            ]
        }
        tilstande = {
            'klassepubliceret': [
                {
                    'publiceret': 'Publiceret',
                    'virkning': validity
                }
            ]
        }

        payload = {
            "attributter": attributter,
            "relationer": relationer,
            "tilstande": tilstande
        }
        response = requests.post(
            url=self.mox_base + '/klassifikation/klasse',
            json=payload
        )
        assert response.status_code == 201
        return response.json()

    def read_employment_changed(self):
        if not self.employment_response:  # Caching mechanism, we need to get of this
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
                'StatusPassiveIndicator': 'false',
                'SalaryAgreementIndicator': 'false',
                'SalaryCodeGroupIndicator': 'false'
            }
            response = sd_lookup(url, params=params)
            self.employment_response = response.get('Person', [])
        return self.employment_response

    def read_person_changed(self):
        params = {
            'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
            'DeactivationDate': self.to_date.strftime('%d.%m.%Y'),
            'StatusActiveIndicator': 'true',
            'StatusPassiveIndicator': 'false',
            'ContactInformationIndicator': 'false',
            'PostalAddressIndicator': 'false'
            # TODO: Er der kunder, som vil udlæse adresse-information?
        }
        url = 'GetPersonChangedAtDate20111201'
        response = sd_lookup(url, params=params)
        return response.get('Person', [])

    def update_changed_persons(self):
        # Så vidt vi ved, består person_changed af navn, cpr nummer og ansættelser.
        # Ansættelser håndteres af update_employment, så vi tjekker for ændringer i
        # navn og opdaterer disse poster. Nye personer oprettes.
        person_changed = self.read_person_changed()
        for person in person_changed:
            # TODO: Shold this go in sd_common?
            given_name = person.get('PersonGivenName', '')
            sur_name = person.get('PersonSurnameName', '')
            sd_name = '{} {}'.format(given_name, sur_name)
            cpr = person['PersonCivilRegistrationIdentifier']

            uuid = None
            mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
            if mo_person:
                if mo_person['name'] == sd_name:
                    return
                uuid = mo_person['uuid']

            payload = {
                "name": sd_name,
                "cpr_no": cpr,
                "org": {
                    "uuid": self.org_uuid
                }
            }
            if uuid:
                payload['uuid'] = uuid

            return_uuid = self.helper._mo_post('e/create', payload).json()
            print('Created or updated employee {} with uuid {}'.format(
                sd_name,
                return_uuid
            ))

    def check_non_existent_departments(self):
        """
        Runs through all changes and checks if all org units exists in MO.
        :return: True if org is self consistent, False if not.
        """
        all_ok = True
        employments_changed = self.read_employment_changed()
        for employment in employments_changed:
            sd_engagement = employment['Employment']
            if not isinstance(sd_engagement, list):
                sd_engagement = [sd_engagement]
            for engagement in sd_engagement:
                departments = engagement.get('EmploymentDepartment')
                if departments:
                    if not isinstance(departments, list):
                        departments = [departments]
                    for department in departments:
                        department_uuid = department['DepartmentUUIDIdentifier']
                        ou = self.helper.read_ou(department_uuid)
                        if 'status' in ou:
                            ut = self.helper.read_classes_in_facet('org_unit_type')
                            for unit_type in ut[0]:
                                if unit_type['user_key'] == 'Orphan': # CONF!!!!!
                                    ut_uuid = unit_type['uuid']
                            all_ok = False # This can go away soon
                            print('Error: {}'.format(department_uuid))

                            payload = {
                                'uuid': department['DepartmentUUIDIdentifier'],
                                'user_key': department['DepartmentIdentifier'],
                                'name': 'Unnamed department',
                                'parent': {'uuid': self.org_uuid},
                                'org_unit_type': {'uuid': ut_uuid},
                                'validity': {
                                    'from': '1900-01-01',
                                    'to': None
                                }
                            }
                            response = self.helper._mo_post('ou/create', payload)
                            assert response.status_code == 201
                            print('Created unit {}'.format(
                                department['DepartmentIdentifier'])
                            )
                                
                        else:
                            print('Success: {}'.format(department_uuid))
        # return all_ok
        return True

    def _compare_dates(self, first_date, second_date, expected_diff=1):
        first = datetime.datetime.strptime(first_date, '%Y-%m-%d')
        second = datetime.datetime.strptime(second_date, '%Y-%m-%d')
        compare = first + datetime.timedelta(days=expected_diff)
        return second == compare

    def _calculate_primary(self):
        # Not quite done...
        non_primary = '2194e621-7c74-4914-a500-85d9237931f6'
        primary = '514d491a-160f-4ac8-8a59-02da04b89049'
        return primary

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

    def engagement_components(self, engagement_info):
        job_id = engagement_info['EmploymentIdentifier']

        components = {}
        status_list = engagement_info.get('EmploymentStatus')
        if status_list:
            if not isinstance(status_list, list):
                status_list = [status_list]
        components['status_list'] = status_list

        professions = engagement_info.get('Profession')
        if professions:
            if not isinstance(professions, list):
                professions = [professions]
        components['professions'] = professions

        departments = engagement_info.get('EmploymentDepartment')
        if departments:
            if not isinstance(departments, list):
                departments = [departments]
        components['departments'] = departments

        components['working_time'] = engagement_info.get('WorkingTime')
        # Employment date is not used for anyting
        components['employment_date'] = engagement_info.get('EmploymentDate')
        return job_id, components

    def create_new_engagement(self, engagement, status):
        """
        Create a new engagement
        """
        job_id, engagement_info = self.engagement_components(engagement)

        # AD integration handled in check for primary engagement.
        print(engagement_info['departments'])

        also_edit = False
        if len(engagement_info['departments']) > 1:
            also_edit = True
        org_unit = engagement_info['departments'][0]['DepartmentUUIDIdentifier']
        
        if len(engagement_info['professions']) > 1:
            also_edit = True
        emp_name = engagement_info['professions'][0]['EmploymentName']

        self._update_professions(emp_name)
        job_function = self.job_functions.get(emp_name)
        validity = self._validity(status)
        engagement_type = self._calculate_primary()
        print('Create engagement validity: {}'.format(validity))
        payload = {
            'type': 'engagement',
            'org_unit': {'uuid': org_unit},
            'person': {'uuid': self.mo_person['uuid']},
            'job_function': {'uuid': job_function},
            'engagement_type': {'uuid': engagement_type},
            'user_key': job_id,
            'validity': validity
        }

        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

        self.mo_engagement = self.helper.read_user_engagement(
            self.mo_person['uuid'],
            read_all=True,
            use_cache=False
        )
        print('Engagement {} created'.format(job_id))

        if also_edit:
            # This will take of the extra entries
            self.edit_engagement(engagement)

    def _terminate_engagement(self, from_date, job_id):
        mo_engagement = self._find_engagement(job_id, from_date)

        if not mo_engagement:
            print('MAJOR PROBLEM: TERMINATING NON-EXISTING JOB!!!!')
            return

        payload = {
            'type': 'engagement',
            'uuid': mo_engagement['uuid'],
            'validity': {'to': from_date}
        }
        response = self.helper._mo_post('details/terminate', payload)
        # TODO!!! Assertion needs to check the content of the 400-reply
        # 404 means that the engagement is no longer active. SD has a strange habbit
        # of sometimes making an explicit termination of an already ended engagement
        # and thus these can be safely ignored.
        assert response.status_code in (200, 400, 404)
        return response

    def _engagement_payload(self, data, mo_engagement):
        # Consider to find the mo_engagement localy
        payload = {
            'type': 'engagement',
            'uuid': mo_engagement['uuid'],
            'data': data
        }
        return payload

    def edit_engagement(self, engagement):
        """
        Edit an engagement
        """
        job_id, engagement_info = self.engagement_components(engagement)
        mo_engagement = self._find_engagement(job_id)

        data = {}
        if engagement_info['departments']:
            for department in engagement_info['departments']:
                org_unit = department['DepartmentUUIDIdentifier']
                validity = self._validity(department)
                data = {'org_unit': {'uuid': org_unit},
                        'validity': validity}
                payload = self._engagement_payload(data, mo_engagement)
                response = self.helper._mo_post('details/edit', payload)
                # TODO!!! Assertion needs to check the content of the 400-reply
                assert response.status_code in (200, 400)
                if response.status_code == 400:
                        print('Department change had no effect')
                print('Changed department of engagement {}'.format(job_id))
        else:
            print('No change en department')

        if engagement_info['professions']:
            if isinstance(engagement_info['professions'], dict):
                professions = [engagement_info['professions']]
            else:
                professions = engagement_info['professions']

            for profession_info in professions:
                # We load the name from SD and handles the AD-integration
                # when calculating the primary engagement.
                emp_name = profession_info['EmploymentName']

                self._update_professions('emp_name')

                job_function = self.job_functions.get(emp_name)
                validity = self._validity(profession_info)
                data = {'job_function': {'uuid': job_function},
                        'validity': validity}
                payload = self._engagement_payload(data, mo_engagement)
                print(emp_name)
                print(payload)
                response = self.helper._mo_post('details/edit', payload)
                # TODO!!! Assertion needs to check the content of the 400-reply
                assert response.status_code in (200, 400)
                if response.status_code == 400:
                    print('Profession change had no effect')
                print('Changed profession of engagement {}'.format(job_id))
        else:
            print('No change in profession')    

        if engagement_info['working_time']:
            if isinstance(engagement_info['working_time'], dict):
                work_times = [engagement_info['working_time']]
            else:
                work_times = engagement_info['working_time']

            for worktime_info in work_times:
                working_time = float(worktime_info['OccupationRate'])
                validity = self._validity(worktime_info)
                data = {'fraction': int(working_time * 1000000),
                        'validity': validity}
                payload = self._engagement_payload(data, mo_engagement)
                response = self.helper._mo_post('details/edit', payload)
                # TODO!!! Assertion needs to check the content of the 400-reply
                assert response.status_code in (200, 400)
                if response.status_code == 400:
                    print('Work time effect change had no effect')
                print('Changed working time of engagement {}'.format(job_id))
        else:
            print('No change in working time')

    def _update_user_employments(self, cpr, sd_engagement):
        for engagement in sd_engagement:
            job_id, eng = self.engagement_components(engagement)

            print('Job id: {}'.format(job_id))
            if job_id == '23878':
                print('This job is new and has no department!!!!!')
                continue

            skip = False
            # If status is present, we have a potential creation
            if eng['status_list']:
                
                for status in eng['status_list']:
                    code = status['EmploymentStatusCode']

                    if code not in ('0', '1', '3', '8', '9', 'S'):
                        print(status)
                        1/0

                    if status['EmploymentStatusCode'] == '0':
                        print('What to do? Cpr: {}, job: {}'.format(cpr, job_id))
                        skip = True

                    if status['EmploymentStatusCode'] == '1':
                        mo_eng = self._find_engagement(job_id)
                        if mo_eng:
                            print('Edit engagegement {}'.format(mo_eng['uuid']))
                            self.edit_engagement(engagement)
                        else:
                            print('Create new engagement')
                            self.create_new_engagement(engagement, status)
                        skip = True

                    if status['EmploymentStatusCode'] == '3':
                        print(engagement)
                        print('Create a leave for {} '.format(cpr))
                        skip = True
                        1/0

                    if status['EmploymentStatusCode'] == '8':
                        from_date = status['ActivationDate']
                        print('Terminate user {}, job_id {} '.format(cpr, job_id))
                        self._terminate_engagement(from_date, job_id)

                    if status['EmploymentStatusCode'] in ('S', '9'):
                        for mo_eng in self.mo_engagement:
                            if mo_eng['user_key'] == job_id:
                                consistent = self._compare_dates(
                                    mo_eng['validity']['to'],
                                    status['ActivationDate']
                                )
                                print('Consistent')
                                assert(consistent)
                                skip = True
                            else:
                                # User was never actually hired
                                print('Engagement deleted: {}'.format(
                                    status['EmploymentStatusCode']
                                ))

            if skip:
                continue
            # If status is not present, we should edit existing employment
            if eng['departments']:
                print('Change in department')
                self.edit_engagement(engagement)

            if eng['professions']:
                # self._update_professions(eng['professions'])
                mo_eng = self._find_engagement(job_id)
                if mo_eng:
                    self.edit_engagement(engagement)
                else:
                    print('Problem with profession update!')
                    print(engagement)
                continue

            if eng['working_time']:
                mo_eng = self._find_engagement(job_id)
                assert mo_eng  # In this case, None would be plain wrong
                self.edit_engagement(engagement)

            if eng['employment_date']:
                # This seems to be redundant information
                pass

    def update_all_employments(self):
        employments_changed = self.read_employment_changed()
        for employment in employments_changed:
            print()
            print('----')
            cpr = employment['PersonCivilRegistrationIdentifier']
            print(cpr)
            
            sd_engagement = employment['Employment']
            if not isinstance(sd_engagement, list):
                sd_engagement = [sd_engagement]
                
            self.mo_person = self.helper.read_user(user_cpr=cpr,
                                                   org_uuid=self.org_uuid)
            if not self.mo_person:
                for employment_info in sd_engagement:
                    assert (employment_info['EmploymentStatus']
                            ['EmploymentStatusCode']) in ('S', '8')
                print('Employment deleted (S) or ended before initial import (8)')
                continue

            self.mo_engagement = self.helper.read_user_engagement(
                self.mo_person['uuid'],
                read_all=True,
                use_cache=False
            )

            update_dates = self._update_user_employments(cpr, sd_engagement)
            # Re-calculate primary after all updates for user has been performed.
            """
            for dates in updated_dates:
            engagements =  self.helper.read_user_engagement(
            mo_person['uuid'],
            at=dates.strftime('%Y-%m-%d'),
            use_cache=False
            )
            """

    def _find_engagement(self, job_id, from_date=None):
        # print('Find engagement, from date: {}'.format(from_date))
        relevant_engagement = None
        user_key = str(int(job_id))

        """
        # Possibly this should be the genral case.
        if from_date:
            mo_engagement = self.helper.read_user_engagement(
                self.mo_person['uuid'],
                read_all=True,
                use_cache=False
            )
        else:
            mo_engagement = self.mo_engagement
        """
        # Write an assertion that verifies that all engagements with same user_key
        # also shares uuid
        for mo_eng in self.mo_engagement:
            if mo_eng['user_key'] == user_key:
                relevant_engagement = mo_eng
        return relevant_engagement

    def _update_professions(self, emp_name):
        # Add new profssions to LoRa
        job_uuid = self.job_functions.get(emp_name)
        if not job_uuid:
            print('New job function: {}'.format(emp_name))
            response = self._add_profession_to_lora(emp_name)
            uuid = response['uuid']
            self.job_functions[emp_name] = uuid
        """
        for profession in professions:
            emp_name = profession['EmploymentName']
            job_uuid = self.job_functions.get(emp_name)
            if not job_uuid:
                print('New job function: {}'.format(emp_name))
                response = self._add_profession_to_lora(emp_name)
                uuid = response['uuid']
                self.job_functions[emp_name] = uuid
        """

if __name__ == '__main__':
    # from_date = datetime.datetime(2019, 2, 15, 0, 0)
    # to_date = datetime.datetime(2019, 2, 16, 0, 0)

    # from_date = datetime.datetime(2019, 2, 16, 0, 0)
    # to_date = datetime.datetime(2019, 2, 17, 0, 0)

    # from_date = datetime.datetime(2019, 2, 17, 0, 0)
    # to_date = datetime.datetime(2019, 2, 18, 0, 0)

    from_date = datetime.datetime(2019, 2, 18, 0, 0)
    to_date = datetime.datetime(2019, 2, 19, 0, 0)

    sd_updater = ChangeAtSD(from_date, to_date)
    sd_updater.update_changed_persons()
    sd_updater.update_all_employments()
