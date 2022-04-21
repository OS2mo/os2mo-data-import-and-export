import asyncio
import datetime
from pathlib import Path

import click
import config
import initial
from dateutil import parser
from los_leder import ManagerImporter
from los_org import OrgUnitImporter
from los_pers import PersonImporter
from los_stam import StamImporter


def get_or_create_import_state(settings: config.Settings) -> datetime.datetime:
    """
    Ensure that the state file exists, and return the date of the last import
    If no import has been run, datetime.min is returned
    """
    state_file_path = Path(settings.import_state_file)
    if not state_file_path.is_file():
        print("Import file not present. Creating.")
        with open(state_file_path, "w") as f:
            earliest = datetime.datetime.min
            f.write(earliest.isoformat())
            return earliest
    else:
        with open(state_file_path, "r") as f:
            last_import = parser.parse(f.read())
            print(f"Last import performed at {last_import}")
            return last_import


def set_import_state(settings: config.Settings, import_date: datetime.datetime):
    """Set contents of import state file to specified date"""
    state_file_path = Path(settings.import_state_file)
    with open(state_file_path, "w") as f:
        import_date_string = import_date.isoformat()
        print(f"Writing timestamp '{import_date_string}' to state")
        f.write(import_date_string)


def run_los_import(settings, last_import):
    loop = asyncio.get_event_loop()

    initial_import = asyncio.ensure_future(initial.perform_initial_setup())
    loop.run_until_complete(initial_import)

    # Import STAM
    stam_import = asyncio.ensure_future(StamImporter(last_import).run())
    loop.run_until_complete(stam_import)

    # Import Org
    org_import = asyncio.ensure_future(OrgUnitImporter().run(last_import))
    loop.run_until_complete(org_import)

    # # Import Person
    person_import = asyncio.ensure_future(PersonImporter().run(last_import))
    loop.run_until_complete(person_import)

    # Import manager
    manager_import = asyncio.ensure_future(ManagerImporter().run(last_import))
    loop.run_until_complete(manager_import)

    loop.close()

    set_import_state(settings, datetime.datetime.now())


@click.command()
@click.option("--import-from-date")
@click.option(
    "--ftp-url",
    help="URL of FTP where CSV files will be retrieved from",
)
@click.option(
    "--ftp-user",
    help="Username to use when logging into FTP server",
)
@click.option(
    "--ftp-pass",
    help="Password to use when logging into FTP server",
)
@click.option(
    "--ftp-folder",
    help="FTP folder where CSV files are retrieved from",
)
@click.option(
    "--import-state-file",
    help="Name of import state file",
)
@click.option(
    "--import-csv-folder",
    help="Path to folder containing CSV files to import. Disables FTP reading",
)
@click.option(
    "--azid-it-system-uuid",
    type=click.UUID,
    help="UUID of MO IT system used for the `AZID` column of `Pers_*.csv` files",
)
def main(**kwargs):
    import_from_date = kwargs.pop("import_from_date", None)
    command_line_options = {key: value for key, value in kwargs.items() if value}
    settings = config.Settings.from_kwargs(**command_line_options)

    if import_from_date:
        last_import = datetime.date.fromisoformat(import_from_date)
    else:
        last_import = get_or_create_import_state(settings)

    return run_los_import(settings, last_import)


if __name__ == "__main__":
    main()  # type: ignore
