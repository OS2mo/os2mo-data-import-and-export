import asyncio
import uuid
from datetime import datetime
from typing import Any
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
from mox_helpers.mox_helper import ElementNotFound
from pydantic import Field

from os2mo_data_import.mox_data_types import Facet
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


class BVNStillingsbetegnelse(StamCSV):
    job_function_uuid: uuid.UUID = Field(alias="BVNStillingBetUUID")
    job_function: str = Field(alias="BVNStillingsbetegnelse")

    @staticmethod
    def get_filename() -> str:
        return "STAM_UUID_BVN_Stillingsbetegnelse.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "engagement_job_function_bvn"

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
    Type[BVNStillingsbetegnelse],
    Type[Lederansvar],
    Type[Lederniveau],
    Type[Ledertype],
    Type[Tilknytningsrolle],
]


class StamImporter:
    """Import all classes and IT systems from CSV files.
    Imports based on predetermined UUIDs, so these functions should be idempotent.
    Only imports if file has been modified after `last_import`.
    """

    def __init__(self, last_import: datetime):
        super().__init__()
        self.last_import = last_import

    async def run(self):
        print("Starting STAM import")
        tasks = [
            self.handle_engagementstype(),
            self.handle_enhedstype(),
            self.handle_itsystem(),
            self.handle_stillingsbetegnelse(),
            self.handle_bvn_stillingsbetegnelse(),
            self.handle_lederansvar(),
            self.handle_lederniveau(),
            self.handle_ledertype(),
            self.handle_tilknytningsrolle(),
        ]
        await asyncio.gather(*tasks)
        print("STAM import done")

    async def handle_engagementstype(self):
        """
        Process the external 'engagement type' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Engagementstype)
        return await self._create_classes_from_csv(Engagementstype, rows)

    async def handle_enhedstype(self):
        """
        Process the external 'unit type' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Enhedstype)
        return await self._create_classes_from_csv(Enhedstype, rows)

    async def handle_itsystem(self):
        """
        Process the external 'it system' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(ITSystem)
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

    async def handle_stillingsbetegnelse(self):
        """
        Process the external 'job_function' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Stillingsbetegnelse)
        return await self._create_classes_from_csv(Stillingsbetegnelse, rows)

    async def handle_bvn_stillingsbetegnelse(self):
        """
        Process the external 'BVNStillingsbetegnelse' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(BVNStillingsbetegnelse)
        return await self._create_classes_from_csv(BVNStillingsbetegnelse, rows)

    async def handle_lederansvar(self):
        """
        Process the external 'Lederansvar' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Lederansvar)
        return await self._create_classes_from_csv(Lederansvar, rows)

    async def handle_lederniveau(self):
        """
        Process the external 'Lederniveau' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Lederniveau)
        return await self._create_classes_from_csv(Lederniveau, rows)

    async def handle_ledertype(self):
        """
        Process the external 'Ledertype' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Ledertype)
        return await self._create_classes_from_csv(Ledertype, rows)

    async def handle_tilknytningsrolle(self):
        """
        Process the external 'Tilknytningsrolle' file and create objects if file is
        newer than last import.
        """
        rows = self._load_csv_if_newer(Tilknytningsrolle)
        return await self._create_classes_from_csv(Tilknytningsrolle, rows)

    def _load_csv_if_newer(
        self, csv_class: StamCSVType
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
            if modified_datetime <= self.last_import:
                return None
            return los_files.read_csv(filename, csv_class)

    async def _create_classes_from_csv(
        self, csv_class: StamCSVType, rows: Optional[List[StamCSVType]] = None
    ) -> None:
        if rows is None:
            return

        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)

        facet_uuid = await self._get_or_create_facet(csv_class, mox_helper)

        # Find class UUIDs currently in facet
        facet_class_uuids = list(
            map(
                uuid.UUID,
                set(
                    await mox_helper._search(
                        "klassifikation",
                        "klasse",
                        {"vilkaarligrel": str(facet_uuid)},
                    )
                ),
            )
        )

        # Find rows to insert (classes in CSV but not yet in LoRa)
        rows_class_uuids = set(row.class_uuid for row in rows)
        rows_to_insert = [
            row for row in rows if row.class_uuid not in facet_class_uuids
        ]

        # Create classes in LoRa
        for row in rows_to_insert:
            klasse = mox_payloads.lora_klasse(
                bvn=row.bvn,
                title=row.title,
                facet_uuid=str(facet_uuid),
                org_uuid=str(uuids.ORG_UUID),
                scope="TEXT",
            )
            await mox_helper.insert_klassifikation_klasse(klasse, str(row.class_uuid))
            print("Created LoRa class %r" % row.class_uuid)

        # Find classes to terminate (classes in LoRa but no longer in CSV file)
        class_uuids_to_terminate = [
            class_uuid
            for class_uuid in facet_class_uuids
            if class_uuid not in rows_class_uuids
        ]

        # Terminate classes in LoRa by setting them as unpublished
        unpublish_payload = self._get_lora_unpublish_payload()
        for class_uuid in class_uuids_to_terminate:
            try:
                await mox_helper._update(
                    "klassifikation",
                    "klasse",
                    str(class_uuid),
                    unpublish_payload,
                )
            except KeyError:  # this class is already unpublished
                print("LoRa class %r was already unpublished" % class_uuid)
            else:
                print("Unpublished LoRa class %r" % class_uuid)

    @staticmethod
    def _get_lora_unpublish_payload():
        return {
            "attributter": {
                "klasseegenskaber": [
                    {
                        "brugervendtnoegle": "-",
                        "titel": "-",
                        "virkning": {
                            "from": "1910-01-01 00:00:00+01",
                            "to": "infinity",
                        },
                    }
                ]
            },
            "tilstande": {
                "klassepubliceret": [
                    {
                        "publiceret": "IkkePubliceret",
                        "virkning": {
                            "from": "1910-01-01 00:00:00+01",
                            "to": "infinity",
                        },
                    }
                ]
            },
        }

    @staticmethod
    async def _get_or_create_facet(csv_class: StamCSVType, mox_helper: Any) -> str:
        facet_bvn = csv_class.get_facet_bvn()

        try:
            # Get facet by its BVN
            facet_uuid = await mox_helper.read_element_klassifikation_facet(
                bvn=facet_bvn
            )
        except ElementNotFound:
            # Facet does not yet exist, create it based on the BVN
            print(f"LoRa facet {facet_bvn!r} not found, creating ...")
            facet_uuid = uuids.uuid_gen(facet_bvn)
            org_uuid = (await mox_helper.read_all_organisation_organisation())[0]
            kls_uuid = (await mox_helper.read_all_klassifikation_klassifikation())[0]
            facet = Facet(
                user_key=facet_bvn,
                uuid=facet_uuid,
                organisation_uuid=org_uuid,
                klassifikation_uuid=kls_uuid,
            )
            await mox_helper.insert_klassifikation_facet(facet.build(), facet_uuid)

        return facet_uuid
