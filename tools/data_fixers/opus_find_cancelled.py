import click
from more_itertools import pairwise
from ra_utils.load_settings import load_settings
from tqdm import tqdm

from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport


def find_cancelled():
    """Find cancelled data and delete it"""
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])
    dumps = opus_helpers.read_available_dumps()
    for date1, date2 in tqdm(
        pairwise(sorted(dumps)), total=len(dumps) - 1, unit="file-pairs"
    ):
        _, _, units, employees = opus_helpers.file_diff(
            dumps[date1], dumps[date2], disable_tqdm=True
        )
        # set enddate to filedate for cancelled employees
        employees = opus_helpers.include_cancelled(dumps[date2], [], employees)
        if units or employees:
            diff = OpusDiffImport(date2, None, {}, filter_ids=filter_ids)
            for employee in employees:
                diff.update_employee(employee)
            diff.handle_filtered_units(units)


@click.command()
def cli():
    """Find Opus units that were cancelled from opus.

    This tool runs through every opus file and looks for cancelled objects.
    """
    find_cancelled()


if __name__ == "__main__":
    cli()
