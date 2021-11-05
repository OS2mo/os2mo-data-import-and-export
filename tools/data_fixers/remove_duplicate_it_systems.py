import json
import pathlib
from collections import ChainMap
from functools import partial
from operator import attrgetter

import click
import requests
from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import ItForbindelse
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker


def get_settings():
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    return json.loads(cfg_file.read_text())


def find_duplicate_it_connections(session):
    # These columns specify uniqueness
    unique_columns = (
        ItForbindelse.it_system_uuid,
        ItForbindelse.bruger_uuid,
        ItForbindelse.enhed_uuid,
        ItForbindelse.brugernavn,
    )
    return (
        session.query(*unique_columns, func.count(ItForbindelse.id))
        .group_by(*unique_columns)
        .having(func.count(ItForbindelse.id) > 1)
        .all()
    )


def find_duplicate_rows(session, duplicate_entry):
    # Unpack row, corresponds to find_duplicate_it_connections select
    it_system_uuid, bruger_uuid, enhed_uuid, brugernavn, _ = duplicate_entry
    # Find duplicate rows by filtering on the duplicated values
    return (
        session.query(ItForbindelse)
        .filter(
            ItForbindelse.it_system_uuid == it_system_uuid,
            ItForbindelse.bruger_uuid == bruger_uuid,
            ItForbindelse.enhed_uuid == enhed_uuid,
            ItForbindelse.brugernavn == brugernavn,
        )
        .order_by(ItForbindelse.startdato)
        .all()
    )


def construct_duplicate_dict(session, duplicate_entry):
    duplicate_rows = find_duplicate_rows(session, duplicate_entry)
    # Remove all but newest entry, as we want to keep 1
    duplicate_rows = duplicate_rows[:-1]
    # Build dict from id --> uuid for all other rows
    row_dict = dict(map(attrgetter("id", "uuid"), duplicate_rows))
    return row_dict


@click.command()
@click.option(
    "--delete",
    is_flag=True,
    default=False,
    type=click.BOOL,
    help="Delete found items from LoRa",
)
def main(delete):
    engine = get_engine()

    # Prepare session
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    # List of tuples, it_sys_uuid, bruger_uuid, enhed_uuid, brugernavn, count
    duplicates = find_duplicate_it_connections(session)
    # List of dicts from id --> uuid (for rows to be deleted)
    duplicate_maps = map(partial(construct_duplicate_dict, session), duplicates)
    # One combined dict from id --> uuid (for rows to be deleted)
    output = dict(ChainMap(*duplicate_maps))

    if delete:
        settings = get_settings()
        delete_from_lora(settings["mox.base"], output.values())
    else:
        # Output delete-map
        print(json.dumps(output, indent=4, sort_keys=True))


def delete_from_lora(mox_base, duplicate_items):
    for uuid in duplicate_items:
        r = requests.delete(
            "{}/organisation/organisationfunktion/{}".format(mox_base, uuid)
        )
        r.raise_for_status()


if __name__ == "__main__":
    main()
