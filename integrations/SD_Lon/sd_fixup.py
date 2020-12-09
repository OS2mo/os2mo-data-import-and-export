import click
from datetime import date

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.SD_Lon.sd_common import sd_lookup, mora_assert
from integrations.SD_Lon.sd_common import primary_types
from integrations.SD_Lon import sd_payloads


def progress_iterator(elements, outputter, mod=10):
    total = len(elements)
    for i, element in enumerate(elements, start=1):
        if i == 1 or i % mod == 0 or i == total:
            outputter("{}/{}".format(i, total))
        yield element


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



def fixup(ctx, mo_employees):
    mora_helper = ctx['mora_helper']
    primary = primary_types(mora_helper)

    if ctx['progress']:
        mo_employees = progress_iterator(mo_employees, print)

    payloads = []
    for mo_employee in mo_employees:
        mo_uuid = mo_employee['uuid']
        mo_engagements = mora_helper.read_user_engagement(
            user=mo_uuid, read_all=True
        )
        no_salary_mo_engagements = filter(
            lambda mo_engagement: mo_engagement['primary']['user_key'] == 'status0',
            mo_engagements
        )
        mo_dict = dict(map(lambda mo_engagement: (mo_engagement['user_key'], mo_engagement), no_salary_mo_engagements))
        if not mo_dict:
            continue

        sd_employments = fetch_user_employments(mo_employee["cpr_no"])
        sd_dict = dict(map(lambda sd_employment: (sd_employment['EmploymentIdentifier'], sd_employment), sd_employments))

        # Find intersection
        common_entries = mo_dict.keys() & sd_dict.keys()
        # TODO: Report non-intersected
        pairs = {x: (mo_dict[x], sd_dict[x]) for x in common_entries}
        
        for key, (mo_engagement, sd_employment) in pairs.items():
            mo_no_salary = (mo_engagement['primary']['user_key'] == 'status0')

            sd_status = sd_employment['EmploymentStatus']['EmploymentStatusCode']
            sd_no_salary = (sd_status == "0")
            if mo_no_salary and not sd_no_salary:
                data = {
                    'validity': mo_engagement['validity'],
                    'primary': {'uuid': primary['non_primary']},
                }
                payload = sd_payloads.engagement(data, mo_engagement)
                payloads.append(payload)
                print("Fixing", key)

    print(len(payloads))
    return

    for payload in payloads:
        response = mora_helper._mo_post('details/edit', payload)
        mora_assert(response)


@click.group()
@click.option('--mora-base', default="http://localhost:5000", help="URL for MO.")
@click.option('--json/--no-json', default=False, help="Output as JSON.")
@click.option('--progress/--no-progress', default=False, help="Print progress.")
@click.option('--fixup-status-0', default=False, help="Attempt to fix status-0 issues.")
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
    fixup(ctx.obj, mo_employees)


@cli.command()
@click.option('--uuid', type=click.UUID, help="UUID of the organisation to check.", required=True)
@click.pass_context
# Example UUID: 'ea5a237a-8f8b-4300-9a00-000006180002'
def fixup_department(ctx, uuid):
    """Fix all employees in an organisation."""
    mo_employees = ctx.obj['mora_helper'].read_organisation_people(
        uuid, read_all=True
    ).keys()
    fixup(ctx.obj, mo_employees)


@cli.command()
@click.option('--limit', type=click.INT, help="Number of employees to check. 0 --> All.", default=5)
@click.pass_context
def fixup_all(ctx, limit):
    """Fix limit/all employees in an MO instance."""
    mo_employees = ctx.obj['mora_helper'].read_all_users(limit=limit)
    fixup(ctx.obj, mo_employees)


if __name__ == '__main__':
    cli()
