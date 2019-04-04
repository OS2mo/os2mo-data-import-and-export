import os
import requests
import datetime
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper
MOX_BASE = os.environ.get('MOX_BASE', None)


#SAML_TOKEN = os.environ.get('SAML_TOKEN', None)


class ChangeAtSD(object):

    def __init__(self, from_date, to_date):
        self.mox_base = MOX_BASE
        self.helper = MoraHelper()
        self.from_date = from_date
        self.to_date = to_date
        self.org_uuid = self.helper.read_organisation()

        self.employment_response = None

        facet_info = self.helper.read_classes_in_facet('engagement_job_function')
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        self.job_functions = {}
        for job in job_functions:
            self.job_functions[job['name']] = job['uuid']

        # If this assertment fails, we will need to re-run the organisation
        # stucture through the normal importer.
        # assert self.check_non_existent_departments()

    def _add_profession_to_lora(self, profession):
        validity = {
            'from': '1900-01-01',
            'to': 'infinity'
        }

        properties = {
            'brugervendtnoegle': profession,
             # "integrationsdata":  # TODO: Check this
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
        if not self.employment_response:
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
            self.employment_resonse = response['Person']
        return self.employment_resonse

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
        return response['Person']

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

            mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
            if mo_person:
                if mo_person['name'] == sd_name:
                    return

            payload = {
                "name": sd_name,
                "cpr_no": cpr,
                "org": {
                    "uuid": self.org_uuid
                }
            }
            print(payload)

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
                            all_ok = False
                            print('Error: {}'.format(department_uuid))
                        else:
                            print('Success: {}'.format(department_uuid))
        return all_ok

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
    
    def update_employments(self):
        employments_changed = self.read_employment_changed()
        for employment in employments_changed:
            cpr = employment['PersonCivilRegistrationIdentifier']

            sd_engagement = employment['Employment']
            if not isinstance(sd_engagement, list):
                sd_engagement = [sd_engagement]

            mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)

            mo_engagement = self.helper.read_user_engagement(
                mo_person['uuid'],
                at=self.from_date.strftime('%Y-%m-%d'),
                use_cache=False
            )
            print()
            print('----')
            for engagement in sd_engagement:
                job_id = engagement['EmploymentIdentifier']
                print('Job id: {}'.format(job_id))
                status_list = engagement.get('EmploymentStatus')
                department = engagement.get('EmploymentDepartment')
                professions = engagement.get('Profession')
                working_time = engagement.get('WorkingTime')
                employment_date = engagement.get('EmploymentDate')
                if status_list:
                    if not isinstance(status_list, list):
                        status_list = [status_list]
                    for status in status_list:
                        code = status['EmploymentStatusCode']

                        if code not in ('0', '1', '3', '8', '9', 'S'):
                            print(status)
                            1/0

                        if status['EmploymentStatusCode'] == '0':
                            print('What to do? Cpr: {}, job: {}'.format(cpr, job_id))

                        if status['EmploymentStatusCode'] == '1':
                            print('Create or edit MO engagement {}'.format(job_id))
                            found_job = False
                            for mo_eng in mo_engagement:
                                if mo_eng['user_key'] == job_id:
                                    found_job = True

                            if found_job:
                                print('EDIT THIS JOB')
                                1/0
                            else:
                                # Here we create an all new job
                                org_unit = department['DepartmentUUIDIdentifier']
                                assert isinstance(professions, dict)
                                self._update_professions(professions)
                                emp_name = professions['EmploymentName']
                                job_function = self.job_functions.get(emp_name)
                                validity = {
                                    'from': status['ActivationDate'],
                                    'to': status['DeactivationDate']
                                }
                                
                                payload = {
                                    'type': 'engagement',
                                    'org_unit': {'uuid': org_unit},
                                    'person': {'uuid': mo_person['uuid']},
                                    'job_function': {'uuid': job_function},
                                    'user_key': job_id,
                                    'validity': validity
                                }
                                # INSERT HERE
                                # print(payload)
                                continue

                        if status['EmploymentStatusCode'] == '3':
                            print('Create a leave for {} '.format(cpr))

                        if status['EmploymentStatusCode'] in ('8', 'S', '9'):
                            for mo_eng in mo_engagement:
                                if mo_eng['user_key'] == job_id:
                                    consistent = self._compare_dates(
                                        mo_eng['validity']['to'],
                                        status['ActivationDate']
                                    )
                                    print('Consistent')
                                    assert(consistent)

                if department:
                    # This field is typically used along with a status change
                    # Jobid 23531 has a department entry with no status change
                    department_uuid = department['DepartmentUUIDIdentifier']
                    # print(self.helper.read_ou(department_uuid))
                    print('Change in department')
                    pass

                if professions:
                    self._update_professions(professions)

                if working_time:
                    # TODO: Here we need to re-calculate primary engagement
                    print(mo_engagement)
                    print(working_time)
                    print('Change in working time')

                if employment_date:
                    # This seems to be redundant information
                    pass
            1/0

    def _update_professions(self, professions):
        # If the profession has changed, this will be a list
        if not isinstance(professions, list):
            professions = [professions]
        for profession in professions:
            emp_name = profession['EmploymentName']
            job_uuid = self.job_functions.get(emp_name)
            #if not job_uuid:
            #    print('New job function: {}'.format(emp_name))
            #    # uuid = self._add_profession_to_lora(emp_name)
            #    uuid = self._add_profession_to_lora('KLAF')
            #    self.job_functions[emp_name] = uuid

            # Now, if we need to act on a profession change
            # This is the time:
        if len(professions) > 1:
            print('Change in profession')
            pass

if __name__ == '__main__':
    from_date = datetime.datetime(2019, 2, 15, 0, 0)
    to_date = datetime.datetime(2019, 2, 25, 0, 0)

    # from_date = datetime.datetime(2019, 2, 26, 0, 0)
    # to_date = datetime.datetime(2019, 2, 27, 0, 0)

    sd_updater = ChangeAtSD(from_date, to_date)
    #sd_updater.update_changed_persons()
    sd_updater.update_employments()
