import uuid
from datetime import date
from datetime import datetime
from operator import itemgetter
from typing import Iterable

import payloads as mo_payloads
import pydantic
import util
from ra_utils.generate_uuid import uuid_generator


class Manager(pydantic.BaseModel):
    OrgUUID: uuid.UUID
    CPR: str  # CPR for associated employee
    LedertypeUUID: uuid.UUID
    LederniveauUUID: uuid.UUID
    LederansvarUUID: uuid.UUID


class ManagerImporter:
    def __init__(self):
        self.cpr_cache = {}
        self.uuid_generator = uuid_generator("AAK")

    def cache_cpr(self):
        """Read all employees from OS2mo and cache them based on their CPR no."""
        print("Caching employees")
        employees = util.lookup_employees()
        cache = map(itemgetter("cpr_no", "uuid"), employees)
        self.cpr_cache = dict(cache)

    def generate_manager_payload(self, manager: Manager) -> dict:
        person_uuid = self.cpr_cache.get(manager.CPR)
        if not person_uuid:
            print(f"No person found for CPR {manager.CPR}")

        return mo_payloads.create_manager(
            uuid=self.uuid_generator(f"{manager.OrgUUID}{manager.CPR}manager"),
            person_uuid=person_uuid,
            org_unit_uuid=manager.OrgUUID,
            responsibility_uuid=manager.LederansvarUUID,
            manager_level_uuid=manager.LederniveauUUID,
            manager_type_uuid=manager.LedertypeUUID,
            from_date=date.today().isoformat(),
            to_date=None,
        )

    def create_manager_payloads(self, managers: Iterable[Manager]) -> Iterable[dict]:
        return map(self.generate_manager_payload, managers)

    async def handle_create(self, filename: str, filedate: datetime):
        """
        Handle creating new manager functions
        """
        managers = util.read_csv(filename, Manager)

        manager_payloads = self.create_manager_payloads(managers)

        async with util.get_client_session() as session:
            await util.create_details(session, manager_payloads)

    async def handle_edit(self, filename: str, filedate: datetime):
        """
        Handle changes to managers
        """
        pass

    async def handle_terminate(self, filename: str, filedate: datetime):
        """
        Handle termination of managers
        """
        pass

    async def run(self, last_import: datetime):
        print("Starting manager import")
        ftp = util.get_ftp_connector()
        filenames = ftp.nlst()

        self.cache_cpr()

        creates = util.parse_filenames(
            filenames, prefix="Leder_nye", last_import=last_import
        )
        edits = util.parse_filenames(
            filenames, prefix="Leder_ret", last_import=last_import
        )
        terminates = util.parse_filenames(
            filenames, prefix="Leder_luk", last_import=last_import
        )

        for filename, filedate in creates:
            await self.handle_create(filename, filedate)

        for filename, filedate in edits:
            await self.handle_edit(filename, filedate)

        for filename, filedate in terminates:
            await self.handle_terminate(filename, filedate)

        print("Person import done")
