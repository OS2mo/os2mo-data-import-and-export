import asyncio
import json
import logging
import pathlib
import time
from functools import wraps

import click
from aiohttp import BasicAuth, ClientSession, TCPConnector

from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import (
    Adresse,
    Bruger,
    Engagement,
    Enhed,
    Leder,
    Tilknytning,
    KLE,
)
from sqlalchemy import create_engine, event, or_
from sqlalchemy.orm import sessionmaker

from integrations.dar_helper.dar_helper import dar_fetch


LOG_LEVEL = logging.DEBUG
LOG_FILE = "os2phonebook_export.log"

logger = logging.getLogger("os2phonebook_export")


class elapsedtime(object):
    """Context manager for timing operations.

    Example:

        with elapsedtime("sleep"):
            time.sleep(1)

        >>> sleep took 1.001 seconds ( 0.001 seconds)
    
    Args:
        operation (str): Informal name given to the operation.
        rounding (int): Number of decimal seconds to include in output.

    Returns:
        :obj:`ContextManager`: The context manager itself.
    """

    def __init__(self, operation, rounding=3):
        self.operation = operation
        self.rounding = rounding

    def __enter__(self):
        self.start_time_real = time.monotonic()
        self.start_time_cpu = time.process_time()
        return self

    def __exit__(self, type, value, traceback):
        elapsed_real = time.monotonic() - self.start_time_real
        elapsed_cpu = time.process_time() - self.start_time_cpu
        print(
            self.operation,
            "took",
            round(elapsed_real, self.rounding),
            "seconds",
            "(",
            round(elapsed_cpu, self.rounding),
            "seconds",
            ")",
        )


def async_to_sync(f):
    """Decorator to run an async function to completion.

    Example:

        @async_to_sync
        async def sleepy(seconds):
            await sleep(seconds)

        sleepy(5)
    
    Args:
        f (async function): The async function to wrap and make synchronous.

    Returns:
        :obj:`sync function`: The syncronhous function wrapping the async one.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(f(*args, **kwargs))
        return loop.run_until_complete(future)

    return wrapper


@click.group()
def cli():
    # Solely used for command grouping
    pass


@cli.command()
@async_to_sync
async def generate_json():
    # TODO: Async database access
    engine = get_engine()
    # Prepare session
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()
    # Count number of queries
    def query_counter(*_):
        query_counter.count += 1

    query_counter.count = 0
    event.listen(engine, "before_cursor_execute", query_counter)

    # Print number of employees
    total_number_of_employees = session.query(Bruger).count()
    print("Total employees:", total_number_of_employees)

    def enrich_org_units_with_engagements(org_unit_map):
        # Enrich with engagements
        queryset = (
            session.query(Engagement, Bruger)
            .filter(Engagement.bruger_uuid == Bruger.uuid)
            .all()
        )

        for engagement, bruger in queryset:
            if engagement.enhed_uuid not in org_unit_map:
                continue
            engagement_entry = {
                "title": engagement.stillingsbetegnelse_titel,
                "name": bruger.fornavn + " " + bruger.efternavn,
                "uuid": bruger.uuid,
            }
            org_unit_map[engagement.enhed_uuid]["engagements"].append(engagement_entry)
        return org_unit_map

    def enrich_org_units_with_associations(org_unit_map):
        queryset = (
            session.query(Tilknytning, Bruger)
            .filter(Tilknytning.bruger_uuid == Bruger.uuid)
            .all()
        )

        for tilknytning, bruger in queryset:
            if tilknytning.enhed_uuid not in org_unit_map:
                continue
            association_entry = {
                "title": tilknytning.tilknytningstype_titel,
                "name": bruger.fornavn + " " + bruger.efternavn,
                "uuid": bruger.uuid,
            }
            org_unit_map[tilknytning.enhed_uuid]["associations"].append(
                association_entry
            )
        return org_unit_map

    def enrich_org_units_with_management(org_unit_map):
        queryset = (
            session.query(Leder, Bruger).filter(Leder.bruger_uuid == Bruger.uuid).all()
        )

        for leder, bruger in queryset:
            if leder.enhed_uuid not in org_unit_map:
                continue
            management_entry = {
                "title": leder.ledertype_titel,
                "name": bruger.fornavn + " " + bruger.efternavn,
                "uuid": bruger.uuid,
            }
            org_unit_map[leder.enhed_uuid]["management"].append(management_entry)
        return org_unit_map

    def enrich_org_units_with_kles(org_unit_map):
        queryset = (
            session.query(KLE).all()
        )

        for kle in queryset:
            if kle.enhed_uuid not in org_unit_map:
                continue
            if kle.kle_aspekt_titel != 'Udførende':
                continue
            kle_entry = {
                "title": kle.kle_nummer_titel,
                # "name": kle.kle_aspekt_titel,
                "uuid": kle.uuid,
            }
            org_unit_map[kle.enhed_uuid]["kles"].append(kle_entry)
        return org_unit_map

    org_unit_map = {}
    org_unit_queue = set()

    def queue_org_unit(uuid):
        if uuid is None:
            return
        org_unit_queue.add(uuid)

    def fetch_parent_org_units():
        # We trust that heirarchies are somewhat shallow, and thus a query per layer is okay.
        while org_unit_queue:
            query_queue = list(org_unit_queue)
            org_unit_queue.clear()
            queryset = session.query(Enhed).filter(Enhed.uuid.in_(query_queue)).all()
            for enhed in queryset:
                add_org_unit(enhed)

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
            "kles": [],
            "addresses": {
                "DAR": [],
                "PHONE": [],
                "EMAIL": [],
                "EAN": [],
                "PNUMBER": [],
                "WWW": [],
            },
        }
        if unit["parent"]:
            # Add parent to queue for bulk fetching later
            queue_org_unit(enhed.forældreenhed_uuid)

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
                },
            }
            employee_map[employee.uuid] = phonebook_entry
        return employee_map

    def enrich_employees_with_engagements(employee_map):
        # Enrich with engagements
        queryset = (
            session.query(Engagement, Enhed)
            .filter(Engagement.enhed_uuid == Enhed.uuid)
            .all()
        )

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
        queryset = (
            session.query(Tilknytning, Enhed)
            .filter(Tilknytning.enhed_uuid == Enhed.uuid)
            .all()
        )

        for _, enhed in queryset:
            add_org_unit(enhed)

        for tilknytning, enhed in queryset:
            tilknytning_entry = {
                "title": tilknytning.tilknytningstype_titel,
                "name": enhed.navn,
                "uuid": enhed.uuid,
            }
            employee_map[tilknytning.bruger_uuid]["associations"].append(
                tilknytning_entry
            )
        return employee_map

    def enrich_employees_with_management(employee_map):
        # Enrich with management
        queryset = (
            session.query(Leder, Enhed).filter(Leder.enhed_uuid == Enhed.uuid).all()
        )

        for _, enhed in queryset:
            add_org_unit(enhed)

        for leder, enhed in queryset:
            leder_entry = {
                "title": leder.ledertype_titel,
                "name": enhed.navn,
                "uuid": enhed.uuid,
            }
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
                    "OS2MO_IMPORT_ROUTINE - NO_RELATIONS_TO_ORG_UNIT employee={phonebook_entry['uuid']}"
                )
                return False
            return True

        filtered_map = {
            uuid: entry
            for uuid, entry in employee_map.items()
            if filter_function(entry)
        }
        return filtered_map

    async def enrich_org_units_with_addresses(org_unit_map):
        # Enrich with adresses
        queryset = session.query(Adresse).filter(Adresse.enhed_uuid != None)

        return await address_helper(
            queryset, org_unit_map, lambda address: address.enhed_uuid
        )

    async def enrich_employees_with_addresses(employee_map):
        # Enrich with adresses
        queryset = session.query(Adresse).filter(Adresse.bruger_uuid != None)

        return await address_helper(
            queryset, employee_map, lambda address: address.bruger_uuid
        )

    async def address_helper(queryset, entry_map, address_to_uuid):
        da_address_types = {
            "DAR": "DAR",
            "Telefon": "PHONE",
            "E-mail": "EMAIL",
            "EAN": "EAN",
            "P-nummer": "PNUMBER",
            "Url": "WWW",
        }

        dawa_queue = {}

        def process_address(address):
            entry_uuid = address_to_uuid(address)
            if entry_uuid not in entry_map:
                return

            atype = da_address_types[address.adressetype_scope]

            if address.værdi:
                value = address.værdi
            elif address.dar_uuid is not None:
                dawa_queue[address.dar_uuid] = dawa_queue.get(address.dar_uuid, [])
                dawa_queue[address.dar_uuid].append(address)
                return
            else:
                logger.warning("Address: {address.uuid} does not have a value")
                return

            formatted_address = {
                "description": address.adressetype_titel,
                "value": value,
            }

            entry_map[entry_uuid]["addresses"][atype].append(formatted_address)

        queryset = queryset.filter(
            # Only include address types we care about
            Adresse.adressetype_scope.in_(da_address_types.keys())
        ).filter(
            # Do not include secret addresses
            or_(Adresse.synlighed_titel == None, Adresse.synlighed_titel != "Hemmelig")
        )
        for address in queryset.all():
            process_address(address)

        uuids = list(dawa_queue.keys())
        result, missing = await dar_fetch(uuids)
        for dar_uuid, reply in result:
            if "betegnelse" not in reply:
                continue
            value = reply["betegnelse"]
            for address in dawa_queue[dar_uuid]:
                entry_uuid = address_to_uuid(address)
                atype = da_address_types[address.adressetype_scope]

                formatted_address = {
                    "description": address.adressetype_titel,
                    "value": value,
                }

                entry_map[entry_uuid]["addresses"][atype].append(
                    formatted_address
                )
        if missing:
            print(missing, "not found in DAWA")

        return entry_map

    # Employees
    # ----------
    employee_map = {}
    with elapsedtime("fetch_employees"):
        employee_map = fetch_employees(employee_map)
    # NOTE: These 3 queries can run in parallel
    with elapsedtime("enrich_employees_with_engagements"):
        employee_map = enrich_employees_with_engagements(employee_map)
    with elapsedtime("enrich_employees_with_associations"):
        employee_map = enrich_employees_with_associations(employee_map)
    with elapsedtime("enrich_employees_with_management"):
        employee_map = enrich_employees_with_management(employee_map)
    # Filter off employees without engagements, assoications and management
    with elapsedtime("filter_employees"):
        employee_map = filter_employees(employee_map)

    # Org Units
    # ----------
    with elapsedtime("fetch_parent_org_units"):
        fetch_parent_org_units()
    # NOTE: These 3 queries can run in parallel
    with elapsedtime("enrich_org_units_with_engagements"):
        org_unit_map = enrich_org_units_with_engagements(org_unit_map)
    with elapsedtime("enrich_org_units_with_associations"):
        org_unit_map = enrich_org_units_with_associations(org_unit_map)
    with elapsedtime("enrich_org_units_with_management"):
        org_unit_map = enrich_org_units_with_management(org_unit_map)
    with elapsedtime("enrich_org_units_with_kles"):
        org_unit_map = enrich_org_units_with_kles(org_unit_map)

    # Both
    # -----
    # Fire all HTTP requests in parallel (for both employees and org units)
    with elapsedtime("enrich_x_with_addresses"):
        employee_map, org_unit_map = await asyncio.gather(
            enrich_employees_with_addresses(employee_map),
            enrich_org_units_with_addresses(org_unit_map),
        )

    print("Processing took", query_counter.count, "queries")

    # Write files
    # ------------
    # TODO: Asyncio to write both files at once?
    with open("tmp/employees.json", "w") as employees_out:
        json.dump(employee_map, employees_out)

    with open("tmp/org_units.json", "w") as org_units_out:
        json.dump(org_unit_map, org_units_out)


@cli.command()
@async_to_sync
async def transfer_json():
    # Load settings file
    settings = None
    with elapsedtime("loading_settings"):
        cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
        if not cfg_file.is_file():
            raise Exception("No setting file")
        settings = json.loads(cfg_file.read_text())
    # Load JSON
    employee_map = {}
    org_unit_map = {}
    # TODO: Asyncio to write both files at once?
    with elapsedtime("loading_employees"):
        with open("tmp/employees.json", "r") as employees_in:
            employee_map = json.load(employees_in)
    with elapsedtime("loading_org_units"):
        with open("tmp/org_units.json", "r") as org_units_in:
            org_unit_map = json.load(org_units_in)
    print("employees:", len(employee_map))
    print("org units:", len(org_unit_map))
    # Transfer JSON
    base_url = settings.get(
        "exporters.os2phonebook_base_url", "http://localhost:8000/api/"
    )
    username = settings.get("exporters.os2phonebook_basic_auth_user", "dataloader")
    password = settings.get("exporters.os2phonebook_basic_auth_pass", "password1")
    employees_url = settings.get(
        "exporters.os2phonebook_employees_uri", "load-employees"
    )
    org_units_url = settings.get(
        "exporters.os2phonebook_org_units_uri", "load-org-units"
    )
    basic_auth = BasicAuth(username, password)

    async def push_updates(url, payload):
        async with aiohttp_session.post(
            base_url + url, json=payload, auth=basic_auth
        ) as response:
            if response.status != 200:
                logger.warning("OS2Phonebook returned non-200 status code")
            print(await response.text())

    with elapsedtime("push_x"):
        async with ClientSession() as aiohttp_session:
            await asyncio.gather(
                push_updates(employees_url, employee_map),
                push_updates(org_units_url, org_unit_map),
            )


if __name__ == "__main__":
    for name in logging.root.manager.loggerDict:
        if name in ("os2phonebook_export", "LoraCache", "SqlExport"):
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )
    cli()
