from datetime import datetime

import mox_helpers.payloads as mox_payloads
import pydantic
from mox_helpers import mox_util
from os2mo_data_import.mox_data_types import Itsystem

import util
import uuids


class Engagementstype(pydantic.BaseModel):
    EngagementstypeUUID: str
    Engagementstype: str


class Enhedstype(pydantic.BaseModel):
    EnhedstypeUUID: str
    Enhedstype: str


class ITSystem(pydantic.BaseModel):
    ITSystemUUID: str
    Name: str
    Userkey: str


class Stillingsbetegnelse(pydantic.BaseModel):
    StillingBetUUID: str
    Stillingsbetegnelse: str


class StamImporter:
    @staticmethod
    async def handle_engagementstype(last_import: datetime):
        """
        Process the external 'engagement type' file and create objects if file is
        newer than last import.
        """
        filename = "STAM_UUID_Engagementstype.csv"

        if util.get_modified_datetime_for_file(filename) <= last_import:
            return

        rows = util.read_csv(filename, Engagementstype)

        mox_helper = await mox_util.create_mox_helper("http://localhost:8080")
        facet_uuid = await mox_helper.read_element_klassifikation_facet(
            bvn="engagement_type"
        )

        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.Engagementstype,
                title=row.Engagementstype,
                facet_uuid=facet_uuid,
                org_uuid=uuids.ORG_UUID,
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(
                klasse, row.EngagementstypeUUID
            )

    @staticmethod
    async def handle_enhedstype(last_import: datetime):
        """
        Process the external 'unit type' file and create objects if file is
        newer than last import.
        """
        filename = "STAM_UUID_Enhedstype.csv"

        if util.get_modified_datetime_for_file(filename) <= last_import:
            return

        rows = util.read_csv(filename, Enhedstype)

        mox_helper = await mox_util.create_mox_helper("http://localhost:8080")
        facet_uuid = await mox_helper.read_element_klassifikation_facet(
            bvn="org_unit_type"
        )

        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.Enhedstype,
                title=row.Enhedstype,
                facet_uuid=facet_uuid,
                org_uuid=uuids.ORG_UUID,
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(klasse, row.EnhedstypeUUID)

    @staticmethod
    async def handle_itsystem(last_import: datetime):
        """
        Process the external 'it system' file and create objects if file is
        newer than last import.
        """
        filename = "STAM_UUID_ITSystem.csv"

        if util.get_modified_datetime_for_file(filename) <= last_import:
            return

        rows = util.read_csv(filename, ITSystem)
        mox_helper = await mox_util.create_mox_helper("http://localhost:8080")

        for row in rows:
            it_system = Itsystem(
                system_name=row.Name,
                user_key=row.Userkey,
            )
            it_system.organisation_uuid = uuids.ORG_UUID
            uuid = row.ITSystemUUID

            json = it_system.build()
            await mox_helper.insert_organisation_itsystem(json, uuid)

    @staticmethod
    async def handle_stillingsbetegnelse(last_import: datetime):
        """
        Process the external 'job_function' file and create objects if file is
        newer than last import.
        """
        filename = "STAM_UUID_Stillingsbetegnelse.csv"

        if util.get_modified_datetime_for_file(filename) <= last_import:
            return

        rows = util.read_csv(filename, Stillingsbetegnelse)

        mox_helper = await mox_util.create_mox_helper("http://localhost:8080")
        facet_uuid = await mox_helper.read_element_klassifikation_facet(
            bvn="engagement_job_function"
        )

        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.Stillingsbetegnelse,
                title=row.Stillingsbetegnelse,
                facet_uuid=facet_uuid,
                org_uuid=uuids.ORG_UUID,
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(klasse, row.StillingBetUUID)

    async def run(self, last_import: datetime):
        """
        Import all klasser and itsystems from files
        Imports based on predetermined UUIDs, so these functions should be idempotent

        Only imports if file has been modified after last_import
        """
        print("Starting STAM import")
        await self.handle_engagementstype(last_import)
        await self.handle_enhedstype(last_import)
        await self.handle_itsystem(last_import)
        await self.handle_stillingsbetegnelse(last_import)
        print("STAM import done")
