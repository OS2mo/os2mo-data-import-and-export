import aiohttp
import asyncio
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
from tools.default_mo_setup import ensure_default_classes, create_new_root_and_it
from tools.subtreedeleter import SubtreeDeleter
from mox_helpers.utils import async_to_sync


def truncate_db(MOX_BASE : str = "http://localhost:8080") -> None:
    r = requests.get(MOX_BASE + "/db/truncate")
    r.raise_for_status()

@async_to_sync
async def delete_opus_tree(api_token, org_unit_uuid: str, delete_functions: bool = False) -> None:
    async with aiohttp.ClientSession() as session:
        session.headers.update({'session': api_token})
        deleter = SubtreeDeleter(session, org_unit_uuid)
        await deleter.run(org_unit_uuid, delete_functions)

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


def prepare_re_import(settings : list = None, opus_uuid:str = None, truncate: bool = None):
    """Create a MO setup with necessary classes

    Clear MO database, or only the opus-unit with the given uuid.
    Ensure necessary classes exists.
    """
    settings = settings or load_settings()
    api_token = settings.get('crontab.SAML_TOKEN')
    if truncate:
        truncate_db(settings.get("mox.base"))
        #create root org and it systems
        create_new_root_and_it()
    elif opus_uuid:
        delete_opus_tree(api_token, opus_uuid, delete_functions=True)
    ensure_default_classes()


def import_opus(ad_reader=None, import_amount:str = 'one'):
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
        if import_amount == 'one':
            break


@click.command()
@click.option(
    "--import-amount",
    type=click.Choice(["one", "all"], case_sensitive=False),
    required=True,
)
@click.option("--opus-uuid", type=click.STRING, help="Delete Opus subtree with the given uuid")
@click.option("--truncate", is_flag=True, help="Truncate all MO tables - be carefull!")
@click.option(
    "--use-ad", is_flag=True, type=click.BOOL, default=False, help="Read from AD"
)
def clear_and_reload(import_amount, opus_uuid, truncate, use_ad):
    settings = load_settings()
    prepare_re_import(settings=settings, opus_uuid=opus_uuid, truncate=truncate)
    AD = None
    if use_ad:
        AD = ad_reader.ADParameterReader()
        AD.cache_all(print_progress=True)
    import_opus(ad_reader=AD, import_amount=import_amount)

if __name__ == "__main__":
    clear_and_reload()
