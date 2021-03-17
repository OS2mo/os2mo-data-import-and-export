import click
import time
from datetime import datetime
from operator import itemgetter
from pathlib import Path

import requests
from more_itertools import flatten, pairwise, prepend
from tqdm import tqdm
from functools import lru_cache
import constants
from exporters.utils.load_settings import load_settings
from integrations.ad_integration import ad_reader
from integrations.ad_integration.utils import apply
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from tools.default_mo_classes import ensure_default_classes, create_new_root_and_it


def truncate_db(MOX_BASE="http://localhost:8080"):
    r = requests.get(MOX_BASE + "/db/truncate")
    r.raise_for_status()

def read_all_files(filter_ids):
    """Create full list of data to write to MO.

    Searches files pairwise for changes and returns the date of change, units and employees with changes
    """
    dumps = opus_helpers.read_available_dumps()

    export_dates = prepend(None, sorted(dumps.keys()))

    @apply
    def lookup_units_and_employees(date1, date2):
        filename1 = dumps.get(date1)
        filename2 = dumps[date2]
        units, employees = opus_helpers.file_diff(
            filename1, filename2, filter_ids, disable_tqdm=True
        )
        print(f"{date2}: Found {len(units)} units and {len(employees)} employees")
        return date2, units, employees

    return map(lookup_units_and_employees, tqdm(pairwise(export_dates)))


def setup_new_mo(settings=load_settings()):
    """Create fresh MO setup with nessecary classes

    Clear MO database and read through all files to find roles and engagement_types
    """
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    truncate_db(settings.get("mox.base"))

    # Setup classes and root organisation
    create_new_root_and_it()
    ensure_default_classes()


def import_all(ad_reader=None):
    """Do a clean import from all opus files.
    Removes current OS2MO data and creates new installation with all the required classes etc.
    Then reads all opus files from the path defined in settings
    """
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    setup_new_mo(settings=settings)
    employee_mapping = opus_helpers.read_cpr_mapping()
    date_units_and_employees = read_all_files(filter_ids)
    for date, units, employees in date_units_and_employees:
        diff = OpusDiffImport(
            date, ad_reader=ad_reader, employee_mapping=employee_mapping
        )
        diff.start_import(units, employees, include_terminations=True)
        # Write latest successful import to rundb so opus_diff_import can continue from where this ended
        opus_helpers.local_db_insert((date, "Diff update ended: {}"))


@click.command()
@click.option(
    "--import-amount",
    type=click.Choice(["none", "all"], case_sensitive=False),
    required=True,
)
@click.option(
    "--use-ad", is_flag=True, type=click.BOOL, default=False, help="Read from AD"
)
def clear_and_reload(import_amount, use_ad):
    if import_amount == "all":
        AD = None
        if use_ad:
            AD = ad_reader.ADParameterReader()
            AD.cache_all()
        import_all(ad_reader=AD)
    else:
        setup_new_mo()

if __name__ == "__main__":
    clear_and_reload()
