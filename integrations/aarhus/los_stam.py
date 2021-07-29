import uuid
from datetime import datetime

from pydantic import Field

import config
import mox_helpers.payloads as mox_payloads
import pydantic
from mox_helpers.mox_helper import create_mox_helper
from os2mo_data_import.mox_data_types import Itsystem

import util
import uuids


class Engagementstype(pydantic.BaseModel):
    engagement_type_uuid: uuid.UUID = Field(alias="EngagementstypeUUID")
    engagement_type: str = Field(alias="Engagementstype")


class Enhedstype(pydantic.BaseModel):
    org_unit_type_uuid: uuid.UUID = Field(alias="EnhedstypeUUID")
    org_unit_type: str = Field(alias="Enhedstype")


class ITSystem(pydantic.BaseModel):
    it_system_uuid: uuid.UUID = Field(alias="ITSystemUUID")
    name: str = Field(alias="Name")
    user_key: str = Field(alias="Userkey")


class Stillingsbetegnelse(pydantic.BaseModel):
    job_function_uuid: uuid.UUID = Field(alias="StillingBetUUID")
    job_function: str = Field(alias="Stillingsbetegnelse")


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

        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)

        facet_uuid = await mox_helper.read_element_klassifikation_facet(
            bvn="engagement_type"
        )

        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.engagement_type,
                title=row.engagement_type,
                facet_uuid=str(facet_uuid),
                org_uuid=str(uuids.ORG_UUID),
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(
                klasse, str(row.engagement_type_uuid)
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

        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)

        facet_uuid = await mox_helper.read_element_klassifikation_facet(
            bvn="org_unit_type"
        )

        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.org_unit_type,
                title=row.org_unit_type,
                facet_uuid=facet_uuid,
                org_uuid=str(uuids.ORG_UUID),
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(
                klasse, str(row.org_unit_type_uuid)
            )

    @staticmethod
    async def handle_itsystem(last_import: datetime):
        """
        Process the external 'it system' file and create objects if file is
        newer than last import.
        """
        filename = "STAM_UUID_ITSystem.csv"

        if util.get_modified_datetime_for_file(filename) <= last_import:
            return

        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)

        rows = util.read_csv(filename, ITSystem)

        for row in rows:
            it_system = Itsystem(
                system_name=row.name,
                user_key=row.user_key,
            )
            it_system.organisation_uuid = str(uuids.ORG_UUID)
            json = it_system.build()
            await mox_helper.insert_organisation_itsystem(json, str(row.it_system_uuid))

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

        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)

        facet_uuid = await mox_helper.read_element_klassifikation_facet(
            bvn="engagement_job_function"
        )

        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.job_function,
                title=row.job_function,
                facet_uuid=facet_uuid,
                org_uuid=str(uuids.ORG_UUID),
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(
                klasse, str(row.job_function_uuid),
            )

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
