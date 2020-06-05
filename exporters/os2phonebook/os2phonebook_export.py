import json
import atexit
import logging
import pathlib
import urllib.parse
import requests
import time
import asyncio
from aiohttp import ClientSession

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

class elapsedtime(object):
    def __init__(self, operation, rounding=3):
        self.operation = operation
        self.rounding = rounding

    def __enter__(self):
        self.t = time.clock()
        return self

    def __exit__(self, type, value, traceback):
        self.t = time.clock() - self.t
        print(self.operation, "took", round(self.t, self.rounding), "seconds")


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
    print("Total employees:", total_number_of_employees)

    def get_org_unit_engagement_references(uuid):
        queryset = session.query(Engagement, Bruger).filter(
            Engagement.enhed_uuid == uuid
        ).filter(
            Engagement.bruger_uuid == Bruger.uuid
        ).all()

        return [
            {
                "title": engagement.stillingsbetegnelse_titel,
                "name": bruger.fornavn + " " + bruger.efternavn,
                "uuid": bruger.uuid,
            } for engagement, bruger in queryset
        ]

    def get_org_unit_association_references(uuid):
        queryset = session.query(Tilknytning, Bruger).filter(
            Tilknytning.enhed_uuid == uuid
        ).filter(
            Tilknytning.bruger_uuid == Bruger.uuid
        ).all()

        return [
            {
                "title": tilknytning.tilknytningstype_titel,
                "name": bruger.fornavn + " " + bruger.efternavn,
                "uuid": bruger.uuid,
            }
            for tilknytning, bruger in queryset
        ]

    def get_org_unit_manager_references(uuid):
        queryset = session.query(Leder, Bruger).filter(
            Leder.enhed_uuid == uuid
        ).filter(
            Leder.bruger_uuid == Bruger.uuid
        ).all()

        return [
            {
                "title": leder.ledertype_titel,
                "name": bruger.fornavn + " " + bruger.efternavn,
                "uuid": bruger.uuid,
            }
            for leder, bruger in queryset
        ]

    org_unit_map = {}
    def add_org_unit(enhed):
        if enhed.uuid in org_unit_map:
            return

        unit = {
            "uuid": enhed.uuid,
            "name": enhed.navn,
            "parent": enhed.forældreenhed_uuid,
            "engagements": [],
            "associations": [],
            "management": [],
            "addresses": {
                "DAR": [],
                "PHONE": [],
                "EMAIL": [],
                "EAN": [],
                "PNUMBER": [],
                "WWW": [],
            }
        }
        if unit["parent"]:
            add_org_unit(
                session.query(Enhed).filter(
                    Enhed.uuid == unit["parent"]
                ).first()
            )

        org_unit_map[enhed.uuid] = unit

    def fetch_employees(employee_map):
        for employee in session.query(Bruger).all():
            phonebook_entry = {
                "uuid": employee.uuid,
                "surname": employee.efternavn,
                "givenname": employee.fornavn,
                "name": employee.fornavn + " " + employee.efternavn,
                "engagements": [],
                "associations": [],
                "management": [],
                "addresses": {
                    "DAR": [],
                    "PHONE": [],
                    "EMAIL": [],
                    "EAN": [],
                    "PNUMBER": [],
                    "WWW": [],
                }
            }
            employee_map[employee.uuid] = phonebook_entry
        return employee_map

    def enrich_employees_with_engagements(employee_map):
        # Enrich with engagements
        queryset = session.query(Engagement, Enhed).filter(
            Engagement.enhed_uuid == Enhed.uuid
        ).all()

        for _, enhed in queryset:
            add_org_unit(enhed)

        for engagement, enhed in queryset:
            engagement_entry = {
                "title": engagement.stillingsbetegnelse_titel,
                "name": enhed.navn,
                "uuid": enhed.uuid,
            }
            employee_map[engagement.bruger_uuid]["engagements"].append(engagement_entry)
        return employee_map

    def enrich_employees_with_associations(employee_map):
        # Enrich with associations
        queryset = session.query(Tilknytning, Enhed).filter(Tilknytning.enhed_uuid == Enhed.uuid).all()

        for _, enhed in queryset:
            add_org_unit(enhed)

        for tilknytning, enhed in queryset:
            tilknytning_entry = {
                "title": tilknytning.tilknytningstype_titel,
                "name": enhed.navn,
                "uuid": enhed.uuid,
            }
            employee_map[tilknytning.bruger_uuid]["associations"].append(tilknytning_entry)
        return employee_map

    def enrich_employees_with_management(employee_map):
        # Enrich with management
        queryset = session.query(Leder, Enhed).filter(
            Leder.enhed_uuid == Enhed.uuid
        ).all()

        for _, enhed in queryset:
            add_org_unit(enhed)

        for leder, enhed in queryset:
            leder_entry = {"title": leder.ledertype_titel, "name": enhed.navn, "uuid": enhed.uuid}
            employee_map[leder.bruger_uuid]["management"].append(leder_entry)
        return employee_map

    def filter_employees(employee_map):
        def filter_function(phonebook_entry):
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
                    f"OS2MO_IMPORT_ROUTINE - NO_RELATIONS_TO_ORG_UNIT employee={phonebook_entry['uuid']}"
                )
                return False
            return True
        
        filtered_map = {uuid: entry for uuid, entry in employee_map.items() if filter_function(entry)}
        return filtered_map
    
    def enrich_org_units_with_addresses(org_unit_map):
        # Enrich with adresses
        queryset = session.query(Adresse).filter(
            Adresse.enhed_uuid != None
        ).all()

        return address_helper(queryset, org_unit_map, lambda address: address.enhed_uuid)

    def enrich_employees_with_addresses(employee_map):
        # Enrich with adresses
        queryset = session.query(Adresse).filter(
            Adresse.bruger_uuid != None
        ).all()

        return address_helper(queryset, employee_map, lambda address: address.bruger_uuid)

    def address_helper(queryset, entry_map, address_to_uuid):

        da_address_types = {
            "DAR": "DAR",
            "Telefon": "PHONE",
            "E-mail": "EMAIL",
            "EAN": "EAN",
            "P-nummer": "PNUMBER",
            "Url": "WWW",
        }

        async def process_address(address, aiohttp_session):
            entry_uuid = address_to_uuid(address)
            if entry_uuid not in entry_map:
                return

            scope = address.adressetype_scope
            if scope not in da_address_types:
                logger.debug(f"Scope: {scope} does not exist")
                return

            visibility = address.synlighed_titel
            if visibility == "Hemmelig":
                return

            if address.værdi:
                value = address.værdi
            elif address.dar_uuid is not None:
                logger.debug(f"Firing HTTP request for dar_uuid: {address.dar_uuid}")
                url = "https://dawa.aws.dk/adresser/" + address.dar_uuid
                async with aiohttp_session.get(url) as response:
                    if response.status != 200:
                        logger.warning("DAWA returned non-200 status code")
                        return
                    body = await response.json()
                    value = body["adressebetegnelse"]
            else:
                logger.warning(f"Address: {address.uuid} does not have a value")
                return

            formatted_address = {
                "description": address.adressetype_titel,
                "value": value,
            }

            entry_map[entry_uuid]["addresses"][da_address_types[scope]].append(formatted_address)

        async def run():
            # Queue all processing
            tasks = []
            async with ClientSession() as aiohttp_session:
                for address in queryset:
                    task = asyncio.ensure_future(process_address(address, aiohttp_session))
                    tasks.append(task)
                # Await all tasks
                await asyncio.gather(*tasks)

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run())
        loop.run_until_complete(future)

        return entry_map

    # Employees
    #----------
    employee_map = {}
    with elapsedtime("fetch_employees"):
        employee_map = fetch_employees(employee_map)
    with elapsedtime("enrich_employees_with_engagements"):
        employee_map = enrich_employees_with_engagements(employee_map)
    with elapsedtime("enrich_employees_with_associations"):
        employee_map = enrich_employees_with_associations(employee_map)
    with elapsedtime("enrich_employees_with_management"):
        employee_map = enrich_employees_with_management(employee_map)
    with elapsedtime("filter_employees"):
        employee_map = filter_employees(employee_map)
    with elapsedtime("enrich_employees_with_addresses"):
        employee_map = enrich_employees_with_addresses(employee_map)

    # Org Units
    #----------
    with elapsedtime("enrich_org_units_with_addresses"):
        org_unit_map = enrich_org_units_with_addresses(org_unit_map)

    # TODO: Bulk these
    with elapsedtime("enrich_org_units_with_engagements"):
        for uuid, _ in org_unit_map.items():
            org_unit_map[uuid]["engagements"] = get_org_unit_engagement_references(uuid)
    with elapsedtime("enrich_org_units_with_associations"):
        for uuid, _ in org_unit_map.items():
            org_unit_map[uuid]["associations"] = get_org_unit_association_references(uuid)
    with elapsedtime("enrich_org_units_with_management"):
        for uuid, _ in org_unit_map.items():
            org_unit_map[uuid]["management"] = get_org_unit_manager_references(uuid)

    # Write files
    #------------
    with open("tmp/employees.json", "w") as employees_out:
        json.dump(employee_map, employees_out)

    with open("tmp/org_units.json", "w") as org_units_out:
        json.dump(org_unit_map, org_units_out)


@cli.command()
def transfer_json():
    # Load settings file
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No setting file")
    settings = json.loads(cfg_file.read_text())
    # Load JSON
    employee_map = {}
    org_unit_map = {}
    with open("tmp/employees.json", "r") as employees_in:
        employee_map = json.load(employees_in)
    with open("tmp/org_units.json", "r") as org_units_in:
        org_unit_map = json.load(org_units_in)
    print("employees:", len(employee_map))
    print("org units:", len(org_unit_map))
    # Transfer JSON
    username = settings["exporters.os2phonebook_basic_auth_user"]
    password = settings["exporters.os2phonebook_basic_auth_pass"]
    employees_url = settings["exporters.os2phonebook_employees_uri"]
    request = requests.post(employees_url, json=employee_map, auth=(username, password))
    if request.status_code != 200:
        logger.warning("OS2Phonebook returned non-200 status code")
        logger.warning(request.text)
    print(request.text)
 
    org_units_url = settings["exporters.os2phonebook_org_units_uri"]
    request = requests.post(org_units_url, json=org_unit_map, auth=(username, password))
    if request.status_code != 200:
        logger.warning("OS2Phonebook returned non-200 status code")
        logger.warning(request.text)
    print(request.text)


if __name__ == "__main__":
    cli()
