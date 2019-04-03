# import os
import datetime
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper

# SAML_TOKEN = os.environ.get('SAML_TOKEN', None)


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
            if employment['PersonCivilRegistrationIdentifier'] == cpr:
                if not isinstance(employment['Employment'], list):
                    emp_list = [employment['Employment']]
                else:
                    emp_list = employment['Employment']
                for emp in emp_list:
                    print('All key-values:')
                    for key, value in emp.items():
                        # pass
                        print('{}: {}'.format(key, value))
                        print()

                    print('Employment status:')
                    if not isinstance(emp['EmploymentStatus'], list):
                        staus_list = [emp['EmploymentStatus']]
                    else:
                        status_list = emp['EmploymentStatus']

                    for status in status_list:
                        print('EmploymenyStatus: {}'.format(status))
                        # employment_info = read_employments_for_user(cpr, date)
                        # print(employment_info)
                print()


if __name__ == '__main__':
    from_date = datetime.datetime(2019, 2, 15, 0, 0)
    to_date = datetime.datetime(2019, 2, 25, 0, 0)

    # from_date = datetime.datetime(2019, 2, 26, 0, 0)
    # to_date = datetime.datetime(2019, 2, 27, 0, 0)

    sd_updater = ChangeAtSD(from_date, to_date)
    sd_updater.update_changed_persons()
    sd_updater.update_employments()
