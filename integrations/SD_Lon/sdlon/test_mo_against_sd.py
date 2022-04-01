import json
from datetime import datetime
from itertools import starmap
from operator import itemgetter

import click
from more_itertools import flatten
from os2mo_helpers.mora_helpers import MoraHelper

from .sd_common import EmploymentStatus
from .sd_common import load_settings
from .sd_common import sd_lookup


def progress_iterator(elements, outputter, mod=10):
    total = len(elements)
    for i, element in enumerate(elements, start=1):
        if i == 1 or i % mod == 0 or i == total:
            outputter("{}/{}".format(i, total))
        yield element


class TestMOAgainstSd(object):
    def __init__(self):
        self.settings = load_settings()
        self.date = datetime.now()

        self.helper = MoraHelper(hostname=self.settings["mora.base"], use_cache=False)

    def _compare_dates(self, sd_employment, mo_engagement):
        """Check dates for discrepancies."""
        status = EmploymentStatus(
            sd_employment["EmploymentStatus"]["EmploymentStatusCode"]
        )
        # TODO, we should also consider to check for actual date_values.
        # This would need a loop that finds the entire validity of the engagement
        if status in EmploymentStatus.on_payroll():
            # Check that MO agrees that the engagement is active
            is_ok = (
                mo_engagement["validity"]["from"]
                < self.date
                < mo_engagement["validity"]["to"]
            )
            # If false, should be current
            return "dates_current", is_ok
        if status in EmploymentStatus.let_go():
            # employment_end_date = sd_employment['EmploymentStatus']['ActivationDate']
            is_ok = mo_engagement["validity"]["to"] < self.date
            # If false, should have been terminated
            return "dates_past", is_ok
        if status == EmploymentStatus.AnsatUdenLoen:
            return "status0", True
        msg = "Unhandled status, fix this program: " + str(status)
        raise NotImplementedError(msg)

    def _compare_salary(self, sd_employment, mo_engagement):
        """Check salary status for discrepancies."""
        mo_no_salary = mo_engagement["primary"]["user_key"] == "status0"
        sd_status = EmploymentStatus(
            sd_employment["EmploymentStatus"]["EmploymentStatusCode"]
        )
        sd_no_salary = sd_status == EmploymentStatus.AnsatUdenLoen
        is_ok = mo_no_salary == sd_no_salary
        return "salary", is_ok

    def _compare_job_function(self, sd_employment, mo_engagement):
        """Check job function for discrepancies."""
        mo_job_function_id = mo_engagement["job_function"]["user_key"]
        sd_job_position_id = sd_employment["Profession"]["JobPositionIdentifier"]
        is_ok = mo_job_function_id == sd_job_position_id
        return "job_function", is_ok

    def _compare_employments(self, sd_employment, mo_engagement):
        """Check a single employment pair for discrepancies.

        Returns a tuple; id, dictionary on the following form:

        employment_id, {
            "check_name": False
            ...
        }
        """
        employment_id = sd_employment["EmploymentIdentifier"]
        if mo_engagement is None:
            return employment_id, {"no_match": False}

        status_checks = dict(
            [
                self._compare_dates(sd_employment, mo_engagement),
                self._compare_salary(sd_employment, mo_engagement),
                self._compare_job_function(sd_employment, mo_engagement),
            ]
        )
        return employment_id, status_checks

    def check_user(self, mo_uuid):
        """Check a single employee for discrepancies.

        Returns a tuple; uuid, dictionary on the following form:

        user_uuid, {
            'uuid': user_uuid,
            'employments': {
                'employment_id': {
                    "check_name": False,
                    ...
                },
                ...
            },
            'all_ok': False # Boolean for whether all employments were all ok
        }
        """
        mo_user = self.helper.read_user(user_uuid=mo_uuid)
        mo_engagements = self.helper.read_user_engagement(user=mo_uuid, read_all=True)
        # List all distinct engagements, collapsed to one line pr. engagement
        condensed_engagements = {}

        for eng in mo_engagements:
            if eng["validity"]["to"] is None:
                eng["validity"]["to"] = "9999-12-31"
            eng["validity"]["from"] = datetime.strptime(
                eng["validity"]["from"], "%Y-%m-%d"
            )
            eng["validity"]["to"] = datetime.strptime(eng["validity"]["to"], "%Y-%m-%d")

            user_key = eng["user_key"]
            # from_date = datetime.strptime(eng['validity']['from'], '%Y-%m-%d')
            # to_date = datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
            from_date = eng["validity"]["from"]
            to_date = eng["validity"]["to"]
            if user_key in condensed_engagements:
                if from_date < condensed_engagements[user_key]["validity"]["from"]:
                    condensed_engagements[user_key]["validity"]["from"] = from_date
                if to_date > condensed_engagements[user_key]["validity"]["to"]:
                    condensed_engagements[user_key]["validity"]["to"] = to_date
            else:
                condensed_engagements[user_key] = eng

        # Notice, this will not get future engagements
        params = {
            "PersonCivilRegistrationIdentifier": mo_user["cpr_no"],
            "StatusActiveIndicator": "true",
            "StatusPassiveIndicator": "true",
            "DepartmentIndicator": "true",
            "EmploymentStatusIndicator": "true",
            "ProfessionIndicator": "true",
            "WorkingTimeIndicator": "true",
            "UUIDIndicator": "true",
            "SalaryAgreementIndicator": "false",
            "SalaryCodeGroupIndicator": "false",
            "EffectiveDate": self.date.strftime("%d.%m.%Y"),
        }
        sd_employments_response = sd_lookup("GetEmployment20111201", params)
        if "Person" not in sd_employments_response:
            return mo_uuid, {
                "uuid": mo_uuid,
                "employments": {"Person not found in SD": {}},
                "all_ok": False,
            }
        employments = sd_employments_response["Person"]["Employment"]
        if not isinstance(employments, list):
            employments = [employments]

        def create_mo_pair(sd_employment):
            employment_id = sd_employment["EmploymentIdentifier"]
            return sd_employment, condensed_engagements.get(employment_id)

        # NOTE: Currently we only find entries in SD that are not in MO,
        #       Not entries in MO that are not in SD
        pairs = map(create_mo_pair, employments)
        # check_status has the format of employment_id --> dict,
        # where the resulting dict has the format of check_name --> bool,
        # where the boolean is whether the check passed or not
        check_status = dict(starmap(self._compare_employments, pairs))
        return mo_uuid, {
            "uuid": mo_uuid,
            "employments": check_status,
            "all_ok": all(
                # collapse the check_status to a list of booleans
                flatten(checks.values() for checks in check_status.values())
            ),
        }

    def check_department(self, department_uuid):
        """Check all employees in an organisation for discrepancies.

        Returns a tuple; uuid, dictionary on the following form:

        department_uuid, {
            'uuid': department_uuid,
            'users': {
                'user_uuid': {
                    'uuid': user_uuid,
                    'employments': {
                        'employment_id': {
                            "check_name": False,
                            ...
                        },
                        ...
                    },
                    'all_ok': False
                },
                ...
            },
            'all_ok': False # Boolean for whether all users were all ok
        }
        """
        employees = self.helper.read_organisation_people(
            department_uuid, read_all=True
        ).keys()
        users = dict(map(self.check_user, employees))
        return department_uuid, {
            "uuid": department_uuid,
            "users": users,
            "all_ok": all(user["all_ok"] for user in users.values()),
        }

    def check_all(self, limit, progress):
        """Check limit/all employees in an MO instance for discrepancies.

        Returns a dictionary on the following form:

        {
            'users': {
                'user_uuid': {
                    'uuid': user_uuid,
                    'employments': {
                        'employment_id': {
                            "check_name": False,
                            ...
                        },
                        ...
                    },
                    'all_ok': False
                },
                ...
            },
            'all_ok': False # Boolean for whether all users were all ok
        }
        """
        employees = self.helper.read_all_users(limit=limit)
        if progress:
            employees = progress_iterator(employees, print)
        employees = map(itemgetter("uuid"), employees)
        users = dict(map(self.check_user, employees))
        return {
            "users": users,
            "all_ok": all(user["all_ok"] for user in users.values()),
        }

    def _print_status(self, name, status):
        print(name + ":", " " * (20 - len(name)), "\u2713" if status else "x")

    def print_user_result(self, test_result):
        """Pretty-print a result from check_user."""
        print("*" * 55)
        print("Checking employee: {}".format(test_result["uuid"]))
        for engagement_id, checks in sorted(test_result["employments"].items()):
            print("Checking engagement: {}".format(engagement_id))
            for key, status_check in sorted(checks.items()):
                self._print_status(key, status_check)
            print()
        print("-" * 26)
        self._print_status("all_ok", test_result["all_ok"])
        print("*" * 55)

    def print_all_result(self, test_result):
        for user_uuid, user_result in sorted(test_result["users"].items()):
            self.print_user_result(user_result)
        print()
        self._print_status("total_all_ok", test_result["all_ok"])

    def print_department_result(self, test_result):
        print("Checking department {}".format(test_result["uuid"]))
        print()
        self.print_all_result(test_result)


@click.group()
@click.option("--json/--no-json", default=False, help="Output as JSON.")
@click.option("--progress/--no-progress", default=False, help="Print progress.")
@click.pass_context
def cli(ctx, json, progress):
    """MO to SD equivalence testing utility.

    Compares MO against SD to find discrepancies in the data.
    """
    # ensure that ctx.obj exists and is a dict, no matter how it is called.
    ctx.ensure_object(dict)
    ctx.obj["json"] = json
    ctx.obj["progress"] = progress


@cli.command()
@click.option(
    "--uuid", type=click.UUID, help="UUID of the user to check.", required=True
)
@click.pass_context
# Example UUID: 'fadfcc38-5d42-4857-a950-0adc65babb13'
def check_user(ctx, uuid):
    """Check a single employee."""
    tester = TestMOAgainstSd()
    _, test_result = tester.check_user(str(uuid))

    if ctx.obj["json"]:
        print(json.dumps(test_result, indent=4, sort_keys=True))
    else:
        tester.print_user_result(test_result)


@cli.command()
@click.option(
    "--uuid", type=click.UUID, help="UUID of the organisation to check.", required=True
)
@click.pass_context
# Example UUID: 'ea5a237a-8f8b-4300-9a00-000006180002'
def check_department(ctx, uuid):
    """Check all employees in an organisation."""
    tester = TestMOAgainstSd()
    _, test_result = tester.check_department(str(uuid), ctx.obj["progress"])

    if ctx.obj["json"]:
        print(json.dumps(test_result, indent=4, sort_keys=True))
    else:
        tester.print_department_result(test_result)


@cli.command()
@click.option(
    "--limit",
    type=click.INT,
    help="Number of employees to check. 0 --> All.",
    default=5,
)
@click.pass_context
def check_all(ctx, limit):
    """Check limit/all employees in an MO instance."""
    tester = TestMOAgainstSd()
    test_result = tester.check_all(limit, ctx.obj["progress"])

    if ctx.obj["json"]:
        print(json.dumps(test_result, indent=4, sort_keys=True))
    else:
        tester.print_all_result(test_result)


if __name__ == "__main__":
    cli()
