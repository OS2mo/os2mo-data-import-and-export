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
        # print(self.helper.read_organisation())

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
        }
        url = 'GetPersonChangedAtDate20111201'
        response = sd_lookup(url, params=params)
        return response['Person']

    def update_changed_persons(self):
        person_changed = self.read_person_changed()
        for person in person_changed:
            print('--------')
            cpr = person['PersonCivilRegistrationIdentifier']
            print(person)

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
    # sd_updater.update_employments()
