import uuid
from datetime import datetime
from typing import List
from typing import Optional
from typing import Type
from typing import Union

import config
import los_files
import mox_helpers.payloads as mox_payloads
import pydantic
import uuids
from mox_helpers.mox_helper import create_mox_helper
from pydantic import Field

from os2mo_data_import.mox_data_types import Itsystem


class StamCSV(pydantic.BaseModel):
    @staticmethod
    def get_filename() -> str:
        raise NotImplementedError("must be implemented by subclass")

    @staticmethod
    def get_facet_bvn() -> str:
        raise NotImplementedError("must be implemented by subclass")

    @property
    def bvn(self) -> str:
        raise NotImplementedError("must be implemented by subclass")

    @property
    def title(self) -> str:
        raise NotImplementedError("must be implemented by subclass")

    @property
    def class_uuid(self) -> uuid.UUID:
        raise NotImplementedError("must be implemented by subclass")


class Engagementstype(StamCSV):
    engagement_type_uuid: uuid.UUID = Field(alias="EngagementstypeUUID")
    engagement_type: str = Field(alias="Engagementstype")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Engagementstype.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "engagement_type"

    @property
    def bvn(self) -> str:
        return self.engagement_type

    @property
    def title(self) -> str:
        return self.engagement_type

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.engagement_type_uuid


class Enhedstype(StamCSV):
    org_unit_type_uuid: uuid.UUID = Field(alias="EnhedstypeUUID")
    org_unit_type: str = Field(alias="Enhedstype")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Enhedstype.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "org_unit_type"

    @property
    def bvn(self) -> str:
        return self.org_unit_type

    @property
    def title(self) -> str:
        return self.org_unit_type

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.org_unit_type_uuid


class ITSystem(StamCSV):
    it_system_uuid: uuid.UUID = Field(alias="ITSystemUUID")
    name: str = Field(alias="Name")
    user_key: str = Field(alias="Userkey")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_ITSystem.csv"


class Stillingsbetegnelse(StamCSV):
    job_function_uuid: uuid.UUID = Field(alias="StillingBetUUID")
    job_function: str = Field(alias="Stillingsbetegnelse")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Stillingsbetegnelse.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "engagement_job_function"

    @property
    def bvn(self) -> str:
        return self.job_function

    @property
    def title(self) -> str:
        return self.job_function

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.job_function_uuid


class Lederansvar(StamCSV):
    responsibility_uuid: uuid.UUID = Field(alias="LederansvarUUID")
    responsibility: str = Field(alias="Lederansvar")
    loadtime: datetime = Field(alias="Loadtime")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Lederansvar.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "responsibility"

    @property
    def bvn(self) -> str:
        return self.responsibility

    @property
    def title(self) -> str:
        return self.responsibility

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.responsibility_uuid


class Lederniveau(StamCSV):
    level_uuid: uuid.UUID = Field(alias="LederniveauUUID")
    level: str = Field(alias="Lederniveau")
    loadtime: datetime = Field(alias="Loadtime")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Lederniveau.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "manager_level"

    @property
    def bvn(self) -> str:
        return self.level

    @property
    def title(self) -> str:
        return self.level

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.level_uuid


class Ledertype(StamCSV):
    type_uuid: uuid.UUID = Field(alias="LedertypeUUID")
    type: str = Field(alias="Ledertype")
    loadtime: datetime = Field(alias="Loadtime")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Ledertype.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "manager_type"

    @property
    def bvn(self) -> str:
        return self.type

    @property
    def title(self) -> str:
        return self.type

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.type_uuid


class Tilknytningsrolle(StamCSV):
    association_type_uuid: uuid.UUID = Field(alias="TilknytningsrolleUUID")
    role: str = Field(alias="Tilknytningsrolle")
    loadtime: datetime = Field(alias="Loadtime")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_Tilknytningsrolle.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "association_type"

    @property
    def bvn(self) -> str:
        return self.role

    @property
    def title(self) -> str:
        return self.role

    @property
    def class_uuid(self) -> uuid.UUID:
        return self.association_type_uuid


StamCSVType = Union[
    Type[Engagementstype],
    Type[Enhedstype],
    Type[ITSystem],
    Type[Stillingsbetegnelse],
    Type[Lederansvar],
    Type[Lederniveau],
    Type[Ledertype],
    Type[Tilknytningsrolle],
]


class StamImporter:
    @classmethod
    async def handle_engagementstype(cls, last_import: datetime):
        """
        Process the external 'engagement type' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Engagementstype, last_import)
        return await cls._create_classes_from_csv(Engagementstype, rows)

    @classmethod
    async def handle_enhedstype(cls, last_import: datetime):
        """
        Process the external 'unit type' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Enhedstype, last_import)
        return await cls._create_classes_from_csv(Enhedstype, rows)

    @classmethod
    async def handle_itsystem(cls, last_import: datetime):
        """
        Process the external 'it system' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(ITSystem, last_import)
        if rows is None:
            return

        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)
        for row in rows:
            it_system = Itsystem(
                system_name=row.name,  # type: ignore
                user_key=row.user_key,  # type: ignore
            )
            it_system.organisation_uuid = str(uuids.ORG_UUID)
            json = it_system.build()
            await mox_helper.insert_organisation_itsystem(
                json,
                str(row.it_system_uuid),  # type: ignore
            )

    @classmethod
    async def handle_stillingsbetegnelse(cls, last_import: datetime):
        """
        Process the external 'job_function' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Stillingsbetegnelse, last_import)
        return await cls._create_classes_from_csv(Stillingsbetegnelse, rows)

    @classmethod
    async def handle_lederansvar(cls, last_import: datetime):
        """
        Process the external 'Lederansvar' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Lederansvar, last_import)
        return await cls._create_classes_from_csv(Lederansvar, rows)

    @classmethod
    async def handle_lederniveau(cls, last_import: datetime):
        """
        Process the external 'Lederniveau' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Lederniveau, last_import)
        return await cls._create_classes_from_csv(Lederniveau, rows)

    @classmethod
    async def handle_ledertype(cls, last_import: datetime):
        """
        Process the external 'Ledertype' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Ledertype, last_import)
        return await cls._create_classes_from_csv(Ledertype, rows)

    @classmethod
    async def handle_tilknytningsrolle(cls, last_import: datetime):
        """
        Process the external 'Tilknytningsrolle' file and create objects if file is
        newer than last import.
        """
        rows = cls._load_csv_if_newer(Tilknytningsrolle, last_import)
        return await cls._create_classes_from_csv(Tilknytningsrolle, rows)

    @classmethod
    def _load_csv_if_newer(
        cls, csv_class: StamCSVType, last_import: datetime
    ) -> Union[List[StamCSVType], None]:
        filename = csv_class.get_filename()
        fileset = los_files.get_fileset_implementation()
        try:
            modified_datetime = fileset.get_modified_datetime(filename)
        except ValueError:
            # Raised by `fileset.get_modified_datetime` if file could not be
            # found.
            return None
        else:
            if modified_datetime <= last_import:
                return None
            return los_files.read_csv(filename, csv_class)

    @classmethod
    async def _create_classes_from_csv(
        cls, csv_class: StamCSVType, rows: Optional[List[StamCSVType]] = None
    ) -> None:
        if rows is None:
            return
        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)
        facet_bvn = csv_class.get_facet_bvn()
        facet_uuid = await mox_helper.read_element_klassifikation_facet(bvn=facet_bvn)
        for row in rows:
            klasse = mox_payloads.lora_klasse(
                bvn=row.bvn,
                title=row.title,
                facet_uuid=facet_uuid,
                org_uuid=str(uuids.ORG_UUID),
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(klasse, str(row.class_uuid))

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
        await self.handle_lederansvar(last_import)
        await self.handle_lederniveau(last_import)
        await self.handle_ledertype(last_import)
        await self.handle_tilknytningsrolle(last_import)
        print("STAM import done")
