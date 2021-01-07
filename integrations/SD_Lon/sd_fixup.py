import click
import json
from datetime import date

from more_itertools import unzip, flatten, spy

from os2mo_helpers.mora_helpers import MoraHelper
from exporters.utils.progress import print_progress_iterator
from integrations.SD_Lon.sd_common import sd_lookup, mora_assert
from integrations.SD_Lon.sd_common import primary_types
from integrations.SD_Lon import sd_payloads


def fetch_user_employments(cpr):
    # Notice, this will not get future engagements
    params = {
        'PersonCivilRegistrationIdentifier': cpr,
        'StatusActiveIndicator': 'true',
        'StatusPassiveIndicator': 'true',
        'DepartmentIndicator': 'true',
        'EmploymentStatusIndicator': 'true',
        'ProfessionIndicator': 'true',
        'WorkingTimeIndicator': 'true',
        'UUIDIndicator': 'true',
        'SalaryAgreementIndicator': 'false',
        'SalaryCodeGroupIndicator': 'false',
        'EffectiveDate': date.today().strftime('%d.%m.%Y')
    }
    sd_employments_response = sd_lookup('GetEmployment20111201', params)
    if 'Person' not in sd_employments_response:
        return []

    employments = sd_employments_response['Person']['Employment']
    if not isinstance(employments, list):
        employments = [employments]

    return employments


def fixup_status_0(ctx, work_tuples):
    def sd_not_status_0(work_tuple):
        key, mo_engagement, sd_employment = work_tuple
        sd_status = sd_employment['EmploymentStatus']['EmploymentStatusCode']
        return sd_status != "0"

    def generate_payload(work_tuple):
        key, mo_engagement, sd_employment = work_tuple
        print("Fixing status 0 for", key)
        data = {
            'validity': mo_engagement['validity'],
            'primary': {'uuid': primary['non_primary']},
        }
        payload = sd_payloads.engagement(data, mo_engagement)
        return payload

    # Filter all tuples, where the sd_employment has status 0
    work_tuples = filter(sd_not_status_0, work_tuples)
    # At this point, we have a tuple of items which need to be updated / fixed

    # Convert all the remaining tuples to MO payloads
    mora_helper = ctx['mora_helper']
    primary = primary_types(mora_helper)
    payloads = map(generate_payload, work_tuples)

    for payload in payloads:
        if ctx['dry_run']:
            print("Dry-run, posting to details/edit:", payload)
        else:
            response = mora_helper._mo_post('details/edit', payload)
            mora_assert(response)


def fixup_end_date(ctx, work_tuples):
    data_dict = {}
    for key, mo_engagement, sd_employment in work_tuples:
        data_dict[key] = {
            'mo': mo_engagement,
            'sd': sd_employment,
        }
    print(json.dumps(data_dict, indent=4))


def run_fixup(ctx, mo_employees):
    def fetch_mo_engagements(mo_employee):
        mo_uuid = mo_employee['uuid']
        mora_helper = ctx['mora_helper']
        mo_engagements = mora_helper.read_user_engagement(
            user=mo_uuid, read_all=True
        )
        no_salary_mo_engagements = filter(
            lambda mo_engagement: mo_engagement['primary']['user_key'] == 'status0',
            mo_engagements
        )
        mo_dict = dict(map(lambda mo_engagement: (mo_engagement['user_key'], mo_engagement), no_salary_mo_engagements))
        return mo_dict

    def fetch_sd_employments(mo_employee):
        mo_cpr = mo_employee["cpr_no"]
        sd_employments = fetch_user_employments(mo_cpr)
        sd_dict = dict(map(lambda sd_employment: (sd_employment['EmploymentIdentifier'], sd_employment), sd_employments))
        return sd_dict

    def fetch_pairs(mo_employee):
        try:
            mo_dict = fetch_mo_engagements(mo_employee)
            if not mo_dict:
                return None
            sd_dict = fetch_sd_employments(mo_employee)
            if not sd_dict:
                return None
            return mo_dict, sd_dict
        except Exception as exp:
            print(mo_employee)
            print(exp)
            return None

    def process_tuples(mo_dict, sd_dict):
        # Find intersection
        common_keys = mo_dict.keys() & sd_dict.keys()
        for key in common_keys:
            yield (key, mo_dict[key], sd_dict[key])

    if ctx['progress']:
        mo_employees = print_progress_iterator(mo_employees)

    # Dict pair is an iterator of (dict, dict) tuples or None
    # First dict is a mapping from employment_id to mo_engagement
    # Second dict is a mapping from employment_id to sd_engagement
    dict_pairs = map(fetch_pairs, mo_employees)
    # Remove all the None's from dict_pairs
    dict_pairs = filter(None.__ne__, dict_pairs)

    # Check if we are empty, we need data for unzip below
    head, dict_pairs = spy(dict_pairs)
    if head == []:
        print("Unable to create any pairs!")
        return

    # Convert dict_pairs into an iterator of three tuples:
    # (key, mo_engagement, sd_employment)
    # 'key' is the shared employment_id
    work_tuples = flatten(map(process_tuples, *unzip(dict_pairs)))

    if ctx['fixup_status_0']:
        fixup_status_0(ctx, work_tuples)

    if ctx['fixup_end_date']:
        fixup_end_date(ctx, work_tuples)


@click.group()
@click.option('--mora-base', default="http://localhost:5000", help="URL for MO.")
@click.option('--json/--no-json', default=False, help="Output as JSON.")
@click.option('--progress/--no-progress', default=False, help="Print progress.")
@click.option('--fixup-status-0', is_flag=True, default=False, help="Attempt to fix status-0 issues.")
@click.option('--fixup-end-date', is_flag=True, default=False, help="Attempt to fix end-date issues.")
@click.option('--dry-run', is_flag=True, default=False, help="Do not write change, simply echo them.")
@click.pass_context
def cli(ctx, mora_base, **kwargs):
    """Tool to fixup MO entries according to SD data.

    This tool should never be needed, as it indicates issues in the main code.
    It is however needed due to the quality of the main code.
    """
    # ensure that ctx.obj exists and is a dict, no matter how it is called.
    ctx.ensure_object(dict)
    ctx.obj = dict(kwargs)
    ctx.obj['mora_helper'] = MoraHelper(
        hostname=mora_base, use_cache=False
    )


@cli.command()
@click.option('--uuid', type=click.UUID, help="UUID of the user to check.", required=True)
@click.pass_context
# Example UUID: 'fadfcc38-5d42-4857-a950-0adc65babb13'
def fixup_user(ctx, uuid):
    """Fix a single employee."""
    mo_employees = [ctx.obj['mora_helper'].read_user(user_uuid=uuid)]
    run_fixup(ctx.obj, mo_employees)


@cli.command()
@click.option('--uuid', type=click.UUID, help="UUID of the organisation to check.", required=True)
@click.pass_context
# Example UUID: 'ea5a237a-8f8b-4300-9a00-000006180002'
def fixup_department(ctx, uuid):
    """Fix all employees in an organisation."""
    mo_employees = ctx.obj['mora_helper'].read_organisation_people(
        uuid, read_all=True
    ).keys()
    run_fixup(ctx.obj, mo_employees)


@cli.command()
@click.option('--limit', type=click.INT, help="Number of employees to check. 0 --> All.", default=5)
@click.pass_context
def fixup_all(ctx, limit):
    """Fix limit/all employees in an MO instance."""
    mo_employees = ctx.obj['mora_helper'].read_all_users(limit=limit)
    run_fixup(ctx.obj, mo_employees)


if __name__ == '__main__':
    cli()
