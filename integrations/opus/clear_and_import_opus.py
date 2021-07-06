import asyncio
import time
from datetime import datetime
from operator import itemgetter
from pathlib import Path
from typing import Optional

import click
import requests
from more_itertools import first, flatten, pairwise, partition, prepend
from ra_utils.load_settings import load_settings
from tqdm import tqdm

import constants
from integrations.ad_integration import ad_reader
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import import_one
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
    units, _ = opus_helpers.parser(dumps[first_date])
    main_unit = first(units)
    calculated_uuid = opus_helpers.generate_uuid(main_unit["@id"])
    return str(calculated_uuid)


def prepare_re_import(
    settings: Optional[list] = None,
    opus_uuid: Optional[str] = None,
    truncate: Optional[bool] = None,
    connections: int = 4,
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
            opus_uuid,
            delete_functions=True,
            keep_functions=["KLE", "Relateret Enhed"],
            connections=connections,
        )
    ensure_default_classes()


def import_opus(ad_reader=None, import_all: bool = False) -> None:
    """Import one or all files from opus even if no previous files have been imported"""
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])
    dumps = opus_helpers.read_available_dumps()

    export_dates = prepend(None, sorted(dumps.keys()))
    date_pairs = pairwise(export_dates)
    for date1, date2 in date_pairs:

        import_one(ad_reader, date2, date1, dumps, filter_ids)
        if not import_all:
            break


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
    import_all: bool, delete_opus: bool, truncate: bool, use_ad: bool, connections: int
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
    prepare_re_import(
        settings=settings,
        opus_uuid=opus_uuid,
        truncate=truncate,
        connections=connections,
    )
    AD = None
    if use_ad:
        AD = ad_reader.ADParameterReader()
        AD.cache_all(print_progress=True)
    import_opus(ad_reader=AD, import_all=import_all)


if __name__ == "__main__":
    clear_and_reload()
