from datetime import date
from typing import List

import click

from os2mo_data_import.os2mo_helpers.mora_helpers import MoraHelper
from tools.data_fixers.sd_fixup import fetch_user_employments

mora_helper = MoraHelper(use_cache=False)


def compare_single_user(mo_employee: dict):
    mo_engagement = mora_helper.read_user_engagement(user=mo_employee["uuid"])
    sd_employment = fetch_user_employments(
        cpr=mo_employee["cpr_no"],
        params={
            "DepartmentIndicator": "false",
            "ProfessionIndicator": "false",
            "WorkingTimeIndicator": "false",
        }
    )

    print(mo_engagement)
    print(sd_employment)


def compare_mo_to_sd(mo_employee_batch: List[dict]):
    for mo_employee in mo_employee_batch:
        compare_single_user(mo_employee)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--batch-size",
    type=click.INT,
    help="The number of employees to fetch per request from MO",
    default=50
)
@click.option(
    "--max-iterations",
    type=click.INT,
    help="The maximum number of times to compare 'batch-size' users (for testing)",
    default=10000
)
def check_employment(batch_size, max_iterations):
    """
    Compare MO engagement end dates with SD employment end dates for all users
    and log any inconsistencies.

    Args:
          batch_size: The number of employees to fetch from MO at a time.
          max_iterations: The maximum number of times to compare 'batch-size' users
    """

    assert batch_size > 0

    start = 0
    mo_employee_batch = mora_helper.read_all_users(limit=batch_size, start=start)
    while mo_employee_batch and start / batch_size < max_iterations:
        # We have to compare the users in batches since MO will crash otherwise
        # on servers with many (~20000) users
        compare_mo_to_sd(mo_employee_batch)
        start += batch_size
        mo_employee_batch = mora_helper.read_all_users(
            limit=batch_size, start=start
        )


if __name__ == "__main__":
    cli()
