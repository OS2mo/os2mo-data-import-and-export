import json
import pathlib

from datetime import datetime

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.SD_Lon.sd_common import sd_lookup


class TestMoAgainsSd(object):
    def __init__(self):
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())
        self.date = datetime.now()

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)

    def _compare_employments(self, employment, mo_eng):
        # job_position_id = employment['Profession']['JobPositionIdentifier']
        status = employment['EmploymentStatus']['EmploymentStatusCode']
        # occupation_rate = float(employment['WorkingTime']['OccupationRate'])

        # TODO, we should also consider to check for actual date_values.
        # This would need a loop that finds the entire validity of the engagement
        if status in ['1', '3']:
            # Check that MO agrees that the engagement is active
            if mo_eng['validity']['from'] < self.date < mo_eng['validity']['to']:
                print('Engagement {} is correctly current'.format(
                    mo_eng['user_key']))
            else:
                print('Engagement {} should be current!'.format(mo_eng['user_key']))
                exit(1)

        elif status in ['7', '8', '9']:
            # employment_end_date = employment['EmploymentStatus']['ActivationDate']
            if mo_eng['validity']['to'] < self.date:
                print('Engagement {} is correctly in the past'.format(
                    mo_eng['user_key']))
            else:
                print('Engagement {} should have been terminated!'.format(
                    mo_eng['user_key']))
                exit(1)
        else:
            print('Unhandled status, fix this program')
            exit(1)

    def check_user(self, mo_uuid):
        mo_user = self.helper.read_user(user_uuid=mo_uuid)
        mo_engagements = self.helper.read_user_engagement(
            user=mo_uuid, read_all=True, only_primary=True)
        condensed_engagements = {}  # List all distinct engagements, collapsed
        # to one line pr. engagement

        for eng in mo_engagements:
            if eng['validity']['to'] is None:
                eng['validity']['to'] = '9999-12-31'
            eng['validity']['from'] = datetime.strptime(
                eng['validity']['from'], '%Y-%m-%d')
            eng['validity']['to'] = datetime.strptime(
                eng['validity']['to'], '%Y-%m-%d')

            user_key = eng['user_key']
            # from_date = datetime.strptime(eng['validity']['from'], '%Y-%m-%d')
            # to_date = datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
            from_date = eng['validity']['from']
            to_date = eng['validity']['to']
            if user_key in condensed_engagements:
                if from_date < condensed_engagements[user_key]['validity']['from']:
                    condensed_engagements[user_key]['validity']['from'] = from_date
                if to_date > condensed_engagements[user_key]['validity']['to']:
                    condensed_engagements[user_key]['validity']['to'] = to_date
            else:
                condensed_engagements[eng['user_key']] = eng

        # Notice, this will not get future engagements
        params = {
            'PersonCivilRegistrationIdentifier': mo_user['cpr_no'],
            'StatusActiveIndicator': 'true',
            'StatusPassiveIndicator': 'true',
            'DepartmentIndicator': 'true',
            'EmploymentStatusIndicator': 'true',
            'ProfessionIndicator': 'true',
            'WorkingTimeIndicator': 'true',
            'UUIDIndicator': 'true',
            'SalaryAgreementIndicator': 'false',
            'SalaryCodeGroupIndicator': 'false',
            'EffectiveDate': self.date.strftime('%d.%m.%Y')
        }
        sd_employments_response = sd_lookup('GetEmployment20111201', params)
        employments = sd_employments_response['Person']['Employment']
        if not isinstance(employments, list):
            employments = [employments]

        for employment in employments:
            employment_id = employment['EmploymentIdentifier']
            found_mo_eng = False
            # for mo_eng in mo_engagements:
            for mo_eng in condensed_engagements.values():
                if mo_eng['user_key'] == employment_id:
                    found_mo_eng = True
                    self._compare_employments(employment, mo_eng)
            if not found_mo_eng:
                # TODO, check if the engagement is in the skipped_job_id_list
                print('Unable to find {} in MO!'.format(employment_id))
                exit(1)

    def check_department(self, department_uuid):
        employees = self.helper.read_organisation_people(
            department_uuid, read_all=True)
        for employee in employees.keys():
            print('Cheking {}'.format(employee))
            self.check_user(employee)


if __name__ == '__main__':
    tester = TestMoAgainsSd()

    tester.check_department('ea5a237a-8f8b-4300-9a00-000006180002')
