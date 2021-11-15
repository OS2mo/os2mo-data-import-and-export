from typing import List

import click

from os2mo_helpers.mora_helpers import MoraHelper
from tools.data_fixers.sd_fixup import fetch_sd_employments

mora_helper = MoraHelper(use_cache=False)


# def get_mo_employees(limit: int = 0, start: int = 0) -> List[dict]:
#     return mora_helper.read_all_users(limit=limit, start=start)

def compare_mo_to_sd(mo_employee_batch: List[dict]):
    for mo_employee in mo_employee_batch:
        mo_engagement = mora_helper.read_user_engagement(user=mo_employee["uuid"])
        sd_employment = fetch_sd_employments(mo_employee)

        print(mo_engagement)
        print(sd_employment)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--bulk-size",
    type=click.INT,
    help="The number of employees to fetch per request from MO",
    default=50
)
def check_employment(bulk_size):
    """
    Compare MO engagement end dates with SD employment end dates for all users
    and log any inconsistencies.

    # Args:
    #       bulk_size: The number of employees to fetch from MO at a time.
    """

    start = 0
    mo_employee_batch = mora_helper.read_all_users(limit=bulk_size, start=start)
    while mo_employee_batch:
        compare_mo_to_sd(mo_employee_batch)
        start += bulk_size
        mo_employee_batch = mora_helper.read_all_users(
            limit=bulk_size, start=start
        )


if __name__ == "__main__":
    cli()
