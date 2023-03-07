import logging
import uuid
from datetime import date
from datetime import datetime
from functools import partial
from itertools import chain
from typing import Iterable
from typing import List
from typing import Union

import los_files
import payloads as mo_payloads
import util
from more_itertools import partition
from pydantic import BaseModel
from pydantic import Field
from ra_utils.generate_uuid import uuid_generator


logger = logging.getLogger(__name__)


class _ManagerBase(BaseModel):
    org_uuid: uuid.UUID = Field(alias="OrgUUID")
    cpr: str = Field(alias="CPR")  # CPR for associated employee


class ManagerCreate(_ManagerBase):
    manager_type_uuid: uuid.UUID = Field(alias="LedertypeUUID")
    manager_level_uuid: uuid.UUID = Field(alias="LederniveauUUID")
    responsibility_uuid: uuid.UUID = Field(alias="LederansvarUUID")


class ManagerEdit(ManagerCreate):
    # CSV schema is identical to `ManagerCreate`
    pass


class ManagerTerminate(_ManagerBase):
    # CSV schema is identical to `_ManagerBase`
    pass


class ManagerImporter:
    def __init__(self):
        self.cpr_cache = {}
        self.uuid_generator = uuid_generator("AAK")

    def _generate_rel_uuid(self, manager):
        return self.uuid_generator(f"{manager.org_uuid}{manager.cpr}manager")

    def cache_cpr(self):
        """Read all employees from OS2mo and cache them based on their CPR no."""
        logger.info("Caching employees")
        self.cpr_cache = util.build_cpr_map()

    def generate_manager_payload(
        self, manager: Union[ManagerCreate, ManagerEdit]
    ) -> dict:
        person_uuid = self.cpr_cache.get(manager.cpr)
        if not person_uuid:
            logger.error("No person found for CPR %s", manager.cpr[:6])

        return mo_payloads.create_manager(
            uuid=self._generate_rel_uuid(manager),
            person_uuid=person_uuid,
            org_unit_uuid=manager.org_uuid,
            responsibility_uuid=manager.responsibility_uuid,
            manager_level_uuid=manager.manager_level_uuid,
            manager_type_uuid=manager.manager_type_uuid,
            from_date=date.today().isoformat(),
            to_date=None,
        )

    def create_manager_payloads(
        self, managers: List[Union[ManagerCreate, ManagerEdit]]
    ) -> Iterable[dict]:
        return map(self.generate_manager_payload, managers)

    async def handle_create(self, filename: str, filedate: datetime):
        """
        Handle creating new manager functions
        """
        managers = los_files.read_csv(filename, ManagerCreate)
        manager_payloads = self.create_manager_payloads(managers)  # type: ignore
        async with util.get_client_session() as session:
            await util.create_details(session, manager_payloads)

    async def handle_edit(self, filename: str, filedate: datetime):
        """
        Handle changes to managers
        """
        managers = los_files.read_csv(filename, ManagerEdit)
        manager_payloads = self.create_manager_payloads(managers)  # type: ignore
        orgfunk_uuids = set(await util.lookup_organisationfunktion())
        detail_creates, detail_edits = partition(
            lambda payload: payload["uuid"] in orgfunk_uuids, manager_payloads
        )
        converter = partial(
            mo_payloads.convert_create_to_edit, from_date=filedate.date().isoformat()
        )
        edits = map(converter, chain(manager_payloads, detail_edits))
        async with util.get_client_session() as session:
            await util.create_details(session, detail_creates)
            await util.edit_details(session, edits)

    async def handle_terminate(self, filename: str, filedate: datetime):
        """
        Handle termination of managers
        """
        managers = los_files.read_csv(filename, ManagerTerminate)
        payloads = [
            mo_payloads.terminate_detail(
                "manager",
                self._generate_rel_uuid(manager),
                filedate,
            )
            for manager in managers
        ]
        async with util.get_client_session() as session:
            await util.terminate_details(session, payloads)

    async def run(self, last_import: datetime):
        logger.info("Starting manager import")
        filenames = los_files.get_fileset_implementation().get_import_filenames()

        self.cache_cpr()

        creates = los_files.parse_filenames(
            filenames, prefix="Leder_nye", last_import=last_import
        )
        edits = los_files.parse_filenames(
            filenames, prefix="Leder_ret", last_import=last_import
        )
        terminates = los_files.parse_filenames(
            filenames, prefix="Leder_luk", last_import=last_import
        )

        for filename, filedate in creates:
            await self.handle_create(filename, filedate)

        for filename, filedate in edits:
            await self.handle_edit(filename, filedate)

        for filename, filedate in terminates:
            await self.handle_terminate(filename, filedate)

        logger.info("Manager import done")
