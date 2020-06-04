import json
import atexit
import logging
import pathlib
import urllib.parse
import requests

import click

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.sql_export import SqlExport
from exporters.sql_export.sql_table_defs import (
    Base,
    Facet,
    Klasse,
    Bruger,
    Enhed,
    ItSystem,
    LederAnsvar,
    Adresse,
    Engagement,
    Rolle,
    Tilknytning,
    Orlov,
    ItForbindelse,
    Leder,
)


LOG_LEVEL = logging.DEBUG
LOG_FILE = "os2phonebook_export.log"

logger = logging.getLogger("os2phonebook_export")

for name in logging.root.manager.loggerDict:
    if name in ("os2phonebook_export"):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format="%(levelname)s %(asctime)s %(name)s %(message)s",
    level=LOG_LEVEL,
    filename=LOG_FILE,
)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--resolve-dar/--no-resolve-dar", default=False)
@click.option("--historic/--no-historic", default=False)
@click.option("--use-pickle/--no-use-pickle", default=False)
@click.option("--force-sqlite/--no-force-sqlite", default=False)
def sql_export(resolve_dar, historic, use_pickle, force_sqlite):
    # Load settings file
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No setting file")
    settings = json.loads(cfg_file.read_text())
    # Override settings
    settings["exporters.actual_state.type"] = "SQLite"
    settings["exporters.actual_state_historic.type"] = "SQLite"
    settings["exporters.actual_state.db_name"] = "tmp/OS2mo_ActualState"
    settings["exporters.actual_state_historic.db_name"] = "tmp/OS2mo_historic"
    # Generate sql export
    sql_export = SqlExport(
        force_sqlite=force_sqlite, historic=historic, settings=settings,
    )
    sql_export.perform_export(
        resolve_dar=resolve_dar, use_pickle=use_pickle,
    )


@cli.command()
def generate_json():
    db_string = "sqlite:///{}.db".format("tmp/OS2mo_ActualState")
    engine = create_engine(db_string)
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    total_number_of_employees = session.query(Bruger).count()
    print(total_number_of_employees)

    def get_employee_engagement_references(uuid):
        return [
            {
                "title": engagement.stillingsbetegnelse_titel,
                "name": enhed.navn,
                "uuid": enhed.uuid,
            }
            for engagement, enhed in session.query(Engagement, Enhed)
            .filter(Engagement.bruger_uuid == uuid)
            .filter(Engagement.enhed_uuid == Enhed.uuid)
            .all()
        ]

    def get_employee_address_references(uuid):
        address_types = {
            "DAR": [],
            "PHONE": [],
            "EMAIL": [],
            "EAN": [],
            "PNUMBER": [],
            "WWW": [],
        }

        da_address_types = {
            "DAR": "DAR",
            "Telefon": "PHONE",
            "E-mail": "EMAIL",
            "EAN": "EAN",
            "P-nummer": "PNUMBER",
            "Url": "WWW",
        }
        for address in session.query(Adresse).filter(Adresse.bruger_uuid == uuid):
            scope = address.adressetype_scope

            if scope not in da_address_types:
                logger.debug(f"Scope: {scope} does not exist")
                continue

            visibility = address.synlighed_titel
            if visibility == "Hemmelig":
                continue

            if address.værdi:
                value = address.værdi
            elif address.dar_uuid is not None:
                logger.debug("Firing HTTP request for dar_uuid")
                url = "https://dawa.aws.dk/adresser/" + address.dar_uuid
                request = requests.get(url)
                if request.status_code != 200:
                    logger.warning("DAWA returned non-200 status code")
                    logger.warning(request.text)
                    continue
                value = request.json()["adressebetegnelse"]
            else:
                logger.warning(f"Address: {address.uuid} does not have a value")
                continue

            formatted_address = {
                "description": address.adressetype_titel,
                "value": value,
            }

            address_types[da_address_types[scope]].append(formatted_address)

        return address_types

    def get_employee_association_references(uuid):
        return [
            {
                "title": tilknytning.tilknytningstype_titel,
                "name": enhed.navn,
                "uuid": enhed.uuid,
            }
            for tilknytning, enhed in session.query(Tilknytning, Enhed)
            .filter(Tilknytning.bruger_uuid == uuid)
            .filter(Tilknytning.enhed_uuid == Enhed.uuid)
            .all()
        ]

    def get_employee_manager_references(uuid):

        return [
            {"title": leder.ledertype_titel, "name": enhed.navn, "uuid": enhed.uuid,}
            for leder, enhed in session.query(Leder, Enhed)
            .filter(Leder.bruger_uuid == uuid)
            .filter(Leder.enhed_uuid == Enhed.uuid)
            .all()
        ]

    employee_map = {}
    for employee in session.query(Bruger).all():
        uuid = employee.uuid

        phonebook_entry = {}
        phonebook_entry["uuid"] = uuid
        phonebook_entry["surname"] = employee.efternavn
        phonebook_entry["givenname"] = employee.fornavn
        phonebook_entry["name"] = employee.fornavn + " " + employee.efternavn

        # Enrich employee
        phonebook_entry["engagements"] = get_employee_engagement_references(uuid)
        phonebook_entry["associations"] = get_employee_association_references(uuid)
        phonebook_entry["management"] = get_employee_manager_references(uuid)

        # Do NOT import employees without an engagement or association
        # https://redmine.magenta-aps.dk/issues/34812

        # We do however want to import employees with management roles.
        # As an external employee may be a manager for an organisation unit.

        if (
            not phonebook_entry["associations"]
            and not phonebook_entry["engagements"]
            and not phonebook_entry["management"]
        ):
            logger.info(
                "OS2MO_IMPORT_ROUTINE Skip employee due to missing engagements, associations, management"
            )

            # Reference to the skipped employee to debug log
            logger.debug(
                f"OS2MO_IMPORT_ROUTINE - NO_RELATIONS_TO_ORG_UNIT employee={uuid}"
            )

            continue

        phonebook_entry["addresses"] = get_employee_address_references(uuid)

        employee_map[uuid] = phonebook_entry

    with open("tmp/employees.json", "w") as employees_out:
        json.dump(employee_map, employees_out)


@cli.command()
def transfer_json():
    pass


if __name__ == "__main__":
    cli()
