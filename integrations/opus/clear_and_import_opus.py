import asyncio
import time
from datetime import datetime
from functools import lru_cache
from operator import itemgetter
from pathlib import Path

import click
import requests
from more_itertools import flatten, pairwise, prepend
from tqdm import tqdm

import constants
from more_itertools import first
from exporters.utils.load_settings import load_settings
from integrations.ad_integration import ad_reader
from integrations.ad_integration.utils import apply
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from tools.default_mo_setup import create_new_root_and_it, ensure_default_classes
from tools.subtreedeleter import subtreedeleter_helper
from tools.data_fixers.remove_duplicate_classes import check_duplicates_classes


def truncate_db(MOX_BASE: str = "http://localhost:8080") -> None:
    r = requests.get(MOX_BASE + "/db/truncate")
    r.raise_for_status()

def find_opus_name() -> str:
    """Generates uuid for opus root.

    Reads the first available opus file and generates the uuid for the first unit in the file.
    Assumes this is the root organisation of opus.
    """
    dumps = opus_helpers.read_available_dumps()

    first_date = min(sorted(dumps.keys()))
    first_file = opus_helpers.read_dump_data(dumps[first_date])
    main_unit = first(first_file['orgUnit'])
    calculated_uuid = opus_helpers.generate_uuid(main_unit['@id'])
    return str(calculated_uuid)

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


def prepare_re_import(
    settings: list = None, opus_uuid: str = None, truncate: bool = None
):
    """Create a MO setup with necessary classes.

    Clear MO database, or only the opus-unit with the given uuid.
    Ensure necessary classes exists.
    """
    settings = settings or load_settings()
    if truncate:
        truncate_db(settings.get("mox.base"))
        # create root org and it systems
        create_new_root_and_it()
    elif opus_uuid:
        dub = check_duplicates_classes()
        if dub > 0:
            raise Exception("There are duplicate classes, remove them with tools/data_fixers/remove_duplicate_classes.py --delete")
        subtreedeleter_helper(opus_uuid, delete_functions=True, keep_functions=["KLE", "Relateret Enhed"])
    ensure_default_classes()


def import_opus(ad_reader=None, import_all: bool = False):
    """Do a clean import from all opus files.

    Removes current OS2MO data and creates new installation with all the required classes etc.
    Then reads all opus files from the path defined in settings
    """
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    employee_mapping = opus_helpers.read_cpr_mapping()
    date_units_and_employees = read_all_files(filter_ids)
    for date, units, employees in date_units_and_employees:
        diff = OpusDiffImport(
            date, ad_reader=ad_reader, employee_mapping=employee_mapping
        )
        diff.start_import(units, employees, include_terminations=True)
        # Write latest successful import to rundb so opus_diff_import can continue from where this ended
        opus_helpers.local_db_insert((date, "Diff update ended: {}"))
        if not import_all:
            break


@click.command()
@click.option(
    "--import-all",
    is_flag = True,
    default = False,
    help = "Import all opus files. Default is only the first file."
)
@click.option(
    "--delete-opus", is_flag=True, help="Delete Opus subtree"
)
@click.option("--truncate", is_flag=True, help="Truncate all MO tables")
@click.option(
    "--use-ad", is_flag=True, type=click.BOOL, default=False, help="Read from AD"
)
def clear_and_reload(import_all, delete_opus, truncate, use_ad):
    settings = load_settings()
    if truncate:
        click.confirm('This will purge ALL DATA FROM MO. Do you want to continue?', abort=True)
    opus_uuid = find_opus_name() if delete_opus else None
    prepare_re_import(settings=settings, opus_uuid=opus_uuid, truncate=truncate)
    AD = None
    if use_ad:
        AD = ad_reader.ADParameterReader()
        AD.cache_all(print_progress=True)
    import_opus(ad_reader=AD, import_all=import_all)


if __name__ == "__main__":
    clear_and_reload()
