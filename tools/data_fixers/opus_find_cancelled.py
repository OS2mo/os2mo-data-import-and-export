import click
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from integrations.opus.opus_exceptions import UnknownOpusUnit
from more_itertools import pairwise
from ra_utils.load_settings import load_settings
from tqdm import tqdm


def find_cancelled(dry_run):
    """Find cancelled data and delete it"""
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])
    dumps = opus_helpers.read_available_dumps()
    for date1, date2 in tqdm(
        pairwise(sorted(dumps)), total=len(dumps) - 1, unit="file-pairs"
    ):
        file_diffs = opus_helpers.file_diff(
            dumps[date1], dumps[date2], disable_tqdm=True
        )
        units = file_diffs["units"]
        employees, _ = opus_helpers.split_employees_leaves(
            file_diffs["cancelled_employees"]
        )
        # set enddate to filedate for cancelled employees
        employees = opus_helpers.include_cancelled(dumps[date2], [], list(employees))
        if units or employees:
            msg = f"Found {len(units)} units and {len(employees)} employees wich were cancelled on {date2}"
            if dry_run:
                click.echo(msg + "(Dry-run)")
                continue
            click.echo(msg + ". Terminating now.")
            diff = OpusDiffImport(date2, None, {}, filter_ids=filter_ids)
            for employee in employees:
                # Updates each employee with their leave-date overwritten, so that their engagement will be terminated.
                try:
                    diff.update_employee(employee)
                except UnknownOpusUnit:
                    # The unit might be terminated by now, since we're looking through older files. No problem, carry on.
                    continue
            # Handles cancelled units as filtered, wich means terminates them from the date of the file.
            mo_units = diff.find_unterminated_filtered_units(units)
            diff.handle_filtered_units(mo_units)


@click.command()
@click.option(
    "--dry-run",
    type=click.BOOL,
    default=False,
    is_flag=True,
    required=False,
    help="Only prints out the number of units and employees found to have been cancelled",
)
def cli(dry_run):
    """Find Opus units that were cancelled from opus.

    This tool runs through all opus files pairwise and looks for cancelled objects -
    Objects that exist in one file, but not the next.
    Org_units are terminated by treating them as filtered and employees have their
    "leaveDate" set to the date of the file.
    The same happens during regular import now, this script cleans up from earlier versions
    """
    find_cancelled(dry_run)


if __name__ == "__main__":
    cli()
