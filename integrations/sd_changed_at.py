# import os
import datetime
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper

import os
import requests
SAML_TOKEN = os.environ.get('SAML_TOKEN', None)


class ChangeAtSD(object):

    def __init__(self, from_date, to_date):
        self.helper = MoraHelper()
        self.from_date = from_date
        self.to_date = to_date
        self.org_uuid = self.helper.read_organisation()

    def read_employment_changed(self):
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
        return response['Person']

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

    def update_employments(self):
        employments_changed = self.read_employment_changed()
        for employment in employments_changed:
            cpr = employment['PersonCivilRegistrationIdentifier']
            
            sd_engagement = employment['Employment']
            if not isinstance(sd_engagement, list):
                sd_engagement = [sd_engagement]

            #mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
            #mo_engagement = self.helper.read_user_engagement(
            #    mo_person['uuid'],
            #    at=self.from_date.strftime('%Y-%m-%d'),
            #    use_cache=False
            #)
            print()
            print('----')
            for engagement in sd_engagement:
                #print()
                #print(engagement.keys())
                job_id = engagement['EmploymentIdentifier']
                print(job_id)
                status_list = engagement.get('EmploymentStatus', None)
                department = engagement.get('EmploymentDepartment', None)
                profession = engagement.get('Profession', None)
                working_time = engagement.get('WorkingTime', None)
                employment_date = engagement.get('EmploymentDate', None)
                if status_list:
                    if not isinstance(status_list, list):
                        status_list = [status_list]
                    for status in status_list:
                        code = status['EmploymentStatusCode'] 
                        if not code in ('0', '1', '3', '8', '9', 'S'):
                            print(status)
                            1/0
                        if status['EmploymentStatusCode'] == '0':
                            print('What to do? Cpr: {}, job: {}'.format(cpr, job_id))
                        if status['EmploymentStatusCode'] == '1':
                            print('Create or edit MO engagement {}'.format(job_id))
                        if status['EmploymentStatusCode'] == '3':
                            print('Create a leave for {} '.format(cpr))
                        if status['EmploymentStatusCode'] == ('8', 'S', '9'):
                            print('Ensure MO engagement {} ends'.format(job_id))

                if department:
                    # This field is typically used along with a status change
                    # Jobid 23531 has a department entry with no status change
                    # print(department)
                    print('Change in department')
                    pass

                if profession:
                    # print(profession)
                    print('Change in profession')
                    pass

                if working_time:
                    # Here we need to re-calculate primary engagement
                    print('Change in working time')
                    print(working_time)

if __name__ == '__main__':
    from_date = datetime.datetime(2019, 2, 15, 0, 0)
    to_date = datetime.datetime(2019, 2, 25, 0, 0)

    # from_date = datetime.datetime(2019, 2, 26, 0, 0)
    # to_date = datetime.datetime(2019, 2, 27, 0, 0)

    sd_updater = ChangeAtSD(from_date, to_date)
    #sd_updater.update_changed_persons()
    sd_updater.update_employments()
