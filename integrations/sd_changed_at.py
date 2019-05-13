import os
import requests
import datetime
import sd_payloads
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper
MOX_BASE = os.environ.get('MOX_BASE', None)

NO_SALLERY = 'status0'
NON_PRIMARY = 'non-primary'
PRIMARY = 'Ansat'


class ChangeAtSD(object):
    def __init__(self, from_date, to_date=None):
        self.mox_base = MOX_BASE
        self.helper = MoraHelper(use_cache=False)
        self.from_date = from_date
        self.to_date = to_date
        self.org_uuid = self.helper.read_organisation()

        self.employment_response = None

        self.mo_person = None      # Updated continously with the person currently
        self.mo_engagement = None  # being processed.

        engagement_types = self.helper.read_classes_in_facet('engagement_type')
        for engagement_type in engagement_types[0]:
            if engagement_type['user_key'] == PRIMARY:
                self.primary = engagement_type['uuid']
            if engagement_type['user_key'] == NON_PRIMARY:
                self.non_primary = engagement_type['uuid']
            if engagement_type['user_key'] == NO_SALLERY:
                self.no_sallery = engagement_type['uuid']

        ut = self.helper.read_classes_in_facet('org_unit_type')
        for unit_type in ut[0]:
            if unit_type['user_key'] == 'Orphan':  # CONF!!!!!
                self.orphan_uuid = unit_type['uuid']

        facet_info = self.helper.read_classes_in_facet('engagement_job_function')
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        self.job_functions = {}
        for job in job_functions:
            self.job_functions[job['name']] = job['uuid']

        facet_info = self.helper.read_classes_in_facet('leave_type')
        self.leave_uuid = facet_info[0][0]['uuid']
        facet_info = self.helper.read_classes_in_facet('association_type')
        self.association_uuid = facet_info[0][0]['uuid']

        # Create non-existent departments
        self.check_non_existent_departments()

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
            print('No effect')
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
            self.employment_response = response.get('Person', [])
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
        return response.get('Person', [])

    def update_changed_persons(self):
        # Så vidt vi ved, består person_changed af navn, cpr nummer og ansættelser.
        # Ansættelser håndteres af update_employment, så vi tjekker for ændringer i
        # navn og opdaterer disse poster. Nye personer oprettes.
        person_changed = self.read_person_changed()
        print(len(person_changed))
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
                    continue
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
                    print('Created unit {}'.format(
                        department['DepartmentIdentifier'])
                    )
        # Consider to return a status that show if we need to re-run organisation.
        return True

    def _compare_dates(self, first_date, second_date, expected_diff=1):
        first = datetime.datetime.strptime(first_date, '%Y-%m-%d')
        second = datetime.datetime.strptime(second_date, '%Y-%m-%d')
        compare = first + datetime.timedelta(days=expected_diff)
        return second == compare

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
        # print('Find engagement, from date: {}'.format(from_date))
        relevant_engagement = None
        try:
            int(job_id)
            user_key = str(int(job_id)).zfill(5)
        except ValueError:
            user_key = job_id

        print('Find: {}'.format(user_key))

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
        print('Create leave')
        # TODO: This code potentially creates duplicated leaves.
        # Implment solution like the one for associations.
        print('Status: {}'.format(status))
        mo_eng = self._find_engagement(job_id)
        payload = sd_payloads.create_leave(mo_eng, self.mo_person, self.leave_uuid,
                                           job_id, self._validity(status))

        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

    def create_association(self, department, person, job_id, validity):
        """ Create a association for a user """
        print('Create association')
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
            print('Org unit for new engagement: {}'.format(org_unit))
            org_unit = self.apply_NY_logic(org_unit, job_id, validity)
        except IndexError:
            org_unit = '4f79e266-4080-4300-a800-000006180002' # CONF!!!!
            print('WARNING!!! NO UNIT FOR THIS ENGAGEMENT')

        try:
            emp_name = engagement_info['professions'][0]['EmploymentName']
        except KeyError:
            emp_name = 'Ukendt'
        self._update_professions(emp_name)
        payload = sd_payloads.create_engagement(org_unit, self.mo_person,
                                                self.job_functions.get(emp_name),
                                                self.non_primary, job_id,
                                                engagement_info, validity)

        response = self.helper._mo_post('details/create', payload)
        assert response.status_code == 201

        self.mo_engagement = self.helper.read_user_engagement(
            self.mo_person['uuid'],
            read_all=True,
            only_primary=True,
            use_cache=False
        )
        print('Engagement {} created'.format(job_id))

        if also_edit:
            # This will take of the extra entries
            self.edit_engagement(engagement)

    def _terminate_engagement(self, from_date, job_id):
        mo_engagement = self._find_engagement(job_id)

        if not mo_engagement:
            print('MAJOR PROBLEM: TERMINATING NON-EXISTING JOB!!!!')
            return False

        payload = {
            'type': 'engagement',
            'uuid': mo_engagement['uuid'],
            'validity': {'to': from_date}
        }
        response = self.helper._mo_post('details/terminate', payload)
        self._assert(response)
        return True

    def edit_engagement(self, engagement):
        """
        Edit an engagement
        """
        job_id, engagement_info = self.engagement_components(engagement)
        mo_engagement = self._find_engagement(job_id)

        data = {}
        # Here we need to look into the NY-logic
        # we should move users and make associations
        print('Department')
        for department in engagement_info['departments']:
            print('Change department of engagement {}:'.format(job_id))
            org_unit = department['DepartmentUUIDIdentifier']

            mo_eng = self._find_engagement(job_id)
            print('MO ENGAGEMENT VALIDITY: {}'.format(mo_eng['validity']))
            validity = mo_eng['validity']
            # This is the validity of the department, not the engagement
            # validity = self._validity(department)

            associations = self.helper.read_user_association(self.mo_person['uuid'],
                                                             read_all=True)
            current_association = None
            for association in associations:
                if association['user_key'] == job_id:
                    current_association = association['uuid']
            if current_association:
                print('We need to move {}'.format(current_association))
                data = {'org_unit': {'uuid': org_unit},
                        'validity': validity}
                payload = sd_payloads.association(data, current_association)
                response = self.helper._mo_post('details/edit', payload)
                self._assert(response)

            org_unit = self.apply_NY_logic(org_unit, job_id, validity)

            print('Org unit for edited engagement: {}'.format(org_unit))
            data = {'org_unit': {'uuid': org_unit},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        print('Profession')
        for profession_info in engagement_info['professions']:
            print('Change profession of engagement {}'.format(job_id))
            # We load the name from SD and handles the AD-integration
            # when calculating the primary engagement.
            emp_name = profession_info['EmploymentName']
            print('Employment name: {}'.format(emp_name))
            self._update_professions(emp_name)
            job_function = self.job_functions.get(emp_name)

            mo_eng = self._find_engagement(job_id)
            validity = mo_eng['validity']

            data = {'job_function': {'uuid': job_function},
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

        print('Working time')
        for worktime_info in engagement_info['working_time']:
            print('Change working time of engagement {}'.format(job_id))
            working_time = float(worktime_info['OccupationRate'])

            mo_eng = self._find_engagement(job_id)
            validity = mo_eng['validity']

            data = {'fraction': int(working_time * 1000000),
                    'validity': validity}
            payload = sd_payloads.engagement(data, mo_engagement)
            response = self.helper._mo_post('details/edit', payload)
            self._assert(response)

    def _update_user_employments(self, cpr, sd_engagement):
        for engagement in sd_engagement:
            job_id, eng = self.engagement_components(engagement)
            print('Job id: {}'.format(job_id))

            print()
            print(engagement)
            print()
            
            skip = False
            # If status is present, we have a potential creation
            if eng['status_list']:

                for status in eng['status_list']:
                    code = status['EmploymentStatusCode']

                    if code not in ('0', '1', '3', '7', '8', '9', 'S'):
                        print(status)
                        1/0

                    if status['EmploymentStatusCode'] == '0':
                        print('Status 0? Cpr: {}, job: {}'.format(cpr, job_id))
                        mo_eng = self._find_engagement(job_id)
                        if mo_eng:
                            print('Edit engagegement {}'.format(mo_eng['uuid']))
                            self.edit_engagement(engagement)
                        else:
                            print('Create new engagement')
                            self.create_new_engagement(engagement, status)
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
                        print('Create a leave for {} '.format(cpr))
                        self.create_leave(status, job_id)

                    # Should 7 gore here?
                    if status['EmploymentStatusCode'] == '8':
                        from_date = status['ActivationDate']
                        print('Terminate user {}, job_id {} '.format(cpr, job_id))
                        success = self._terminate_engagement(from_date, job_id)
                        if not success:
                            print('Problem wit job-id: {}'.format(job_id))
                            skip = True

                    if status['EmploymentStatusCode'] in ('S', '7', '9'):
                        for mo_eng in self.mo_engagement:
                            if mo_eng['user_key'] == job_id:
                                print(status)
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
            self.edit_engagement(engagement)

    def update_all_employments(self):
        employments_changed = self.read_employment_changed()
        for employment in employments_changed:
            print()
            print('----')
            cpr = employment['PersonCivilRegistrationIdentifier']
            print(cpr)
            print(self.from_date)
            print(self.to_date)

            print(employment)
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
                only_primary=True,
                use_cache=False
            )
            self._update_user_employments(cpr, sd_engagement)
            # Re-calculate primary after all updates for user has been performed.
            print('Calculate primary:')
            self.recalculate_primary()

    def _calculate_rate_and_ids(self, mo_engagement):
        max_rate = 0
        min_id = 9999999
        for eng in mo_engagement:
            if 'user_key' not in eng:
                print('CANNOT CALCULATE PRIMARY!!!')
                print(eng)
                print()
                print(mo_engagement)
                1/0
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
        print(min_id, max_rate)
        return (min_id, max_rate)

    def recalculate_primary(self):
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

        for i in range(0, len(date_list) - 1):
            date = date_list[i]

            mo_engagement = self.helper.read_user_engagement(
                user=uuid,
                at=date,
                only_primary=True,
                use_cache=False
            )
            (min_id, max_rate) = self._calculate_rate_and_ids(mo_engagement)
            if (min_id is None) or (max_rate is None):
                continue

            exactly_one_primary = False
            for eng in mo_engagement:
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
                    print('Primary is: {}'.format(employment_id))
                    exactly_one_primary = True
                    data = {
                        'primary': True,
                        'engagement_type': {'uuid': self.primary},
                        'validity': validity
                    }
                else:
                    print('{} is not primary'.format(employment_id))
                    data = {
                        'primary': False,
                        'engagement_type': {'uuid': self.non_primary},
                        'validity': validity
                    }
                payload = sd_payloads.engagement(data, eng)
                response = self.helper._mo_post('details/edit', payload)
                assert response.status_code in (200, 400)


if __name__ == '__main__':
    from_date = datetime.datetime(2019, 2, 15, 0, 0)
    sd_updater = ChangeAtSD(from_date)
    sd_updater.update_changed_persons()
    sd_updater.update_all_employments()
    del(sd_updater)

    """
    for i in range(0, 30):
        to_date = from_date + datetime.timedelta(days=1)
        sd_updater = ChangeAtSD(from_date, to_date)
        sd_updater.update_changed_persons()
        sd_updater.update_all_employments()
        del(sd_updater)
        from_date = to_date
    """
