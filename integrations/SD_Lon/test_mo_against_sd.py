import json
import pathlib
from enum import Enum
from itertools import starmap
from more_itertools import flatten

from datetime import datetime

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.SD_Lon.sd_common import sd_lookup, primary_types


def starfilter(predicate, iterable):
    for tup in iterable:
        if predicate(*tup):
           yield tup


class EmploymentStatus(Enum):
    """Corresponds to EmploymentStatusCode from SD.

    Employees usually start in AnsatUdenLoen, and then change to AnsatMedLoen.
    This will usually happen once they actually have their first day at work.

    From AnsatMedLoen they can somewhat freely transfer to the other statusses.
    This includes transfering back to AnsatMedLoen from any other status.

    Note for instance, that it is entirely possible to be Ophoert and then get
    hired back, and thus go from Ophoert to AnsatMedLoen.

    There is only one terminal state, namely Slettet, wherefrom noone will
    return. This state is invoked from status 7-8-9 after a few years.

    Status Doed will probably only migrate to status slettet, but there are no
    guarantees given.
    """
    # This status most likely represent not yet being at work
    AnsatUdenLoen = '0'

    # These statusses represent being at work
    AnsatMedLoen = '1'
    Overlov = '3'

    # These statusses represent being let go
    Migreret = '7'
    Ophoert = '8'
    Doed = '9'

    # This status is the special terminal state
    Slettet = 'S'


Employeed = [
    EmploymentStatus.AnsatUdenLoen,
    EmploymentStatus.AnsatMedLoen,
    EmploymentStatus.Overlov
]
LetGo = [
    EmploymentStatus.Migreret,
    EmploymentStatus.Ophoert,
    EmploymentStatus.Doed
]
OnPayroll = [
    EmploymentStatus.AnsatMedLoen,
    EmploymentStatus.Overlov
]


class TestMoAgainsSd(object):
    def __init__(self):
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())
        self.date = datetime.now()

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)

    def _compare_dates(self, sd_employment, mo_engagement):
        status = EmploymentStatus(sd_employment['EmploymentStatus']['EmploymentStatusCode'])
        # TODO, we should also consider to check for actual date_values.
        # This would need a loop that finds the entire validity of the engagement
        if status in OnPayroll:
            # Check that MO agrees that the engagement is active
            is_ok = (mo_engagement['validity']['from'] < self.date < mo_engagement['validity']['to'])
            # If false, should be current
            return "dates_current", is_ok
        if status in LetGo:
            # employment_end_date = sd_employment['EmploymentStatus']['ActivationDate']
            is_ok = (mo_engagement['validity']['to'] < self.date)
            # If false, should have been terminated
            return "dates_past", is_ok
        msg = 'Unhandled status, fix this program: ' + str(status)
        raise NotImplemented(msg)

    def _compare_salary(self, sd_employment, mo_engagement):
        mo_no_salary = (mo_engagement['primary']['user_key'] == 'status0')
        sd_status = EmploymentStatus(
            sd_employment['EmploymentStatus']['EmploymentStatusCode']
        )
        sd_no_salary = (sd_status == EmploymentStatus.AnsatUdenLoen)
        is_ok = (mo_no_salary == sd_no_salary)
        return "salary", is_ok

    def _compare_job_function(self, sd_employment, mo_engagement):
        mo_job_function_id = mo_engagement['job_function']['user_key']
        sd_job_position_id = sd_employment['Profession']['JobPositionIdentifier']
        is_ok = (mo_job_function_id == sd_job_position_id)
        return "job_function", is_ok

    def _compare_employments(self, sd_employment, mo_engagement):
        employment_id = sd_employment['EmploymentIdentifier']
        if mo_engagement is None:
            return employment_id, {'no_match': False}

        status_checks = dict([
            self._compare_dates(sd_employment, mo_engagement),
            self._compare_salary(sd_employment, mo_engagement),
            self._compare_job_function(sd_employment, mo_engagement),
        ])
        return employment_id, status_checks

    def check_user(self, mo_uuid):
        mo_user = self.helper.read_user(user_uuid=mo_uuid)
        mo_engagements = self.helper.read_user_engagement(
            user=mo_uuid, read_all=True
        )
        # List all distinct engagements, collapsed to one line pr. engagement
        condensed_engagements = {}

        for eng in mo_engagements:
            if eng['validity']['to'] is None:
                eng['validity']['to'] = '9999-12-31'
            eng['validity']['from'] = datetime.strptime(
                eng['validity']['from'], '%Y-%m-%d'
            )
            eng['validity']['to'] = datetime.strptime(
                eng['validity']['to'], '%Y-%m-%d'
            )

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
                condensed_engagements[user_key] = eng

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

        def create_mo_pair(sd_employment):
            employment_id = sd_employment['EmploymentIdentifier']
            return sd_employment, condensed_engagements.get(employment_id)

        pairs = map(create_mo_pair, employments)
	# check_status has the format of employment_id --> dict,
        # where the resulting dict has the format of check_name --> bool,
        # where the boolean is whether the check passed or not
        check_status = dict(starmap(self._compare_employments, pairs))
        return mo_uuid, {
            'uuid': mo_uuid,
            'employments': check_status,
            'all_ok': all(
	        # collapse the check_status to a list of booleans
                flatten(checks.values() for checks in check_status.values())
            )
        }

    def check_department(self, department_uuid):
        employees = self.helper.read_organisation_people(
            department_uuid, read_all=True
        )
        users = dict(map(self.check_user, employees.keys()))
        return department_uuid, {
            'uuid': department_uuid,
            'users': users,
            'all_ok': all(user['all_ok'] for user in users.values()),
        }

    def _print_status(self, name, status):
        print(name, " " * (20 - len(name)), u'\u2713' if status else 'x')

    def print_user_result(self, test_result):
        """Pretty-print a result from check_user."""
        print("*" * 45)
        print("Checking {}".format(test_result['uuid']))
        for engagement_id, checks in test_result['employments'].items():
            print("Checking engagement: {}".format(engagement_id))
            for key, status_check in checks.items():
                self._print_status(key, status_check)
            print()
        print("-" * 26)
        self._print_status("all_ok", test_result['all_ok'])
        print("*" * 45)

    def print_department_result(self, test_result):
        print("Checking {}".format(test_result['uuid']))
        print()
        for user_result in test_result['users'].values():
            self.print_user_result(user_result)
        print()
        self._print_status("all_ok", test_result['all_ok'])


if __name__ == '__main__':
    tester = TestMoAgainsSd()

    import json
    _, test_result = tester.check_user('fadfcc38-5d42-4857-a950-0adc65babb13')
    print(json.dumps(test_result, indent=4))
    tester.print_user_result(test_result)

    _, test_result = tester.check_department('ea5a237a-8f8b-4300-9a00-000006180002')
    print(json.dumps(test_result, indent=4))
    tester.print_department_result(test_result)
