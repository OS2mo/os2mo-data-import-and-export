import asyncio
import time
from datetime import datetime
from functools import lru_cache
from itertools import islice
from operator import itemgetter
from pathlib import Path
from typing import Optional

import click
import requests
from more_itertools import first, flatten, pairwise, prepend
from tqdm import tqdm

import constants
from ra_utils.load_settings import load_settings
from integrations.ad_integration import ad_reader
from integrations.ad_integration.utils import apply
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from tools.data_fixers.class_tools import find_duplicates_classes
from tools.default_mo_setup import create_new_root_and_it, ensure_default_classes
from tools.subtreedeleter import subtreedeleter_helper


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
    units, _ = opus_helpers.parser(dumps[first_date], [])
    main_unit = first(units)
    calculated_uuid = opus_helpers.generate_uuid(main_unit["@id"])
    return str(calculated_uuid)


def read_all_files(filter_ids):
    """Create full list of data to write to MO.

    Searches files pairwise for changes and returns the date of change, units and employees with changes
    """
    dumps = opus_helpers.read_available_dumps()

    # Prepend None to be able to start from the first file
    export_dates = prepend(None, sorted(dumps.keys()))
    date_pairs = pairwise(export_dates)

    @apply
    def lookup_units_and_employees(date1, date2):
        filename1 = dumps.get(date1)
        filename2 = dumps[date2]
        units, employees = opus_helpers.file_diff(
            filename1, filename2, filter_ids, disable_tqdm=True
        )
        return date2, units, employees

    return map(lookup_units_and_employees, date_pairs)


def prepare_re_import(
    settings: Optional[list] = None,
    opus_uuid: Optional[str] = None,
    truncate: Optional[bool] = None,
    connections: int = 4
) -> None:
    """Create a MO setup with necessary classes.

    Clear MO database, or only the opus-unit with the given uuid.
    Ensure necessary classes exists.
    """
    settings = settings or load_settings()
    mox_base = settings.get("mox.base")
    if truncate:
        truncate_db(mox_base)
        # Create root org and it systems
        create_new_root_and_it()
    elif opus_uuid:
        session = requests.session()
        dub = find_duplicates_classes(session=session, mox_base=mox_base)
        if dub:
            raise Exception(
                "There are duplicate classes, remove them with tools/data_fixers/remove_duplicate_classes.py --delete"
            )
        subtreedeleter_helper(
            opus_uuid, delete_functions=True, keep_functions=["KLE", "Relateret Enhed"], connections=connections
        )
    ensure_default_classes()


def import_opus(ad_reader=None, import_all: bool = False) -> None:
    """Do a clean import from all opus files.

    Removes current OS2MO data and creates new installation with all the required classes etc.
    Then reads all opus files from the path defined in settings
    """
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    employee_mapping = opus_helpers.read_cpr_mapping()
    date_units_and_employees = read_all_files(filter_ids)
    date_units_and_employees = islice(
        date_units_and_employees, None if import_all else 1
    )
    for date, units, employees in date_units_and_employees:
        print(
            f"Importing from {date}: Found {len(units)} units and {len(employees)} employees"
        )
        diff = OpusDiffImport(
            date, ad_reader=ad_reader, employee_mapping=employee_mapping
        )
        filtered_units, units = opus_helpers.filter_units(units, filter_ids)
        diff.start_import(units, employees, include_terminations=True)
        diff.handle_filtered_units(filtered_units)
        # Write latest successful import to rundb so opus_diff_import can continue from where this ended
        opus_helpers.local_db_insert((date, "Diff update ended: {}"))


@click.command()
@click.option(
    "--import-all",
    is_flag=True,
    help="Import all opus files. Default is only the first file.",
)
@click.option(
    "--delete-opus",
    is_flag=True,
    help="Delete Opus subtree. Deletes all units, engagements etc, but not KLE and related units",
)
@click.option("--truncate", is_flag=True, help="Truncate all MO tables")
@click.option("--use-ad", is_flag=True, help="Read from AD")
@click.option(
    "--connections",
    default=4,
    help="The amount of concurrent requests made to OS2mo",
)
def clear_and_reload(
    import_all: bool, delete_opus: bool, truncate: bool, use_ad: bool, connections:int
) -> None:
    """Tool for reimporting opus files.

    This tool will load the first opus-file into MO if no inputs are given.
    If the opus organisation allready exists, use either --delete-opus or --truncate.
    --delete-opus will use an uuid genereted from the first unit of the first opus-file and delete all units under this unit, and all relations to thoose units except KLE and related units.
    --truncate will truncate the database entirely.
    Add the --use-ad flag to connect to AD when reading users.
    """
    settings = load_settings()
    if truncate:
        click.confirm(
            "This will purge ALL DATA FROM MO. Do you want to continue?", abort=True
        )
    opus_uuid = find_opus_name() if delete_opus else None
    prepare_re_import(settings=settings, opus_uuid=opus_uuid, truncate=truncate, connections=connections)
    AD = None
    if use_ad:
        AD = ad_reader.ADParameterReader()
        AD.cache_all(print_progress=True)
    import_opus(ad_reader=AD, import_all=import_all)


if __name__ == "__main__":
    clear_and_reload()
