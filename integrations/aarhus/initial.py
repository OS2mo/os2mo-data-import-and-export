import asyncio
from datetime import datetime
from uuid import UUID

import config
import pydantic
import uuids
from aiohttp import ClientSession
from more_itertools import one
from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import ElementNotFound
from raclients.lora import ModelClient as LoraModelClient
from ramodels.lora.klasse import Klasse

from os2mo_data_import import ImportHelper  # type: ignore
from os2mo_data_import.mox_data_types import Itsystem


class ClassData(pydantic.BaseModel):
    facet_user_key: str
    user_key: str
    title: str
    uuid: UUID
    scope: str


CLASSES = [
    # Org unit address types
    ClassData(
        title="Postadresse",
        facet_user_key="org_unit_address_type",
        scope="DAR",
        user_key="AddressMailUnit",
        uuid=uuids.UNIT_POSTADDR,
    ),
    ClassData(
        title="LOS ID",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="LOSID",
        uuid=uuids.UNIT_LOS,
    ),
    ClassData(
        title="CVR nummer",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="CVRUnit",
        uuid=uuids.UNIT_CVR,
    ),
    ClassData(
        title="EAN nummer",
        facet_user_key="org_unit_address_type",
        scope="EAN",
        user_key="EANUnit",
        uuid=uuids.UNIT_EAN,
    ),
    ClassData(
        title="P-nummer",
        facet_user_key="org_unit_address_type",
        scope="PNUMBER",
        user_key="PNumber",
        uuid=uuids.UNIT_PNR,
    ),
    ClassData(
        title="SE-nummer",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="SENumber",
        uuid=uuids.UNIT_SENR,
    ),
    ClassData(
        title="IntDebitor-Nr",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="intdebit",
        uuid=uuids.UNIT_DEBITORNR,
    ),
    ClassData(
        title="WWW",
        facet_user_key="org_unit_address_type",
        scope="WWW",
        user_key="UnitWeb",
        uuid=uuids.UNIT_WWW,
    ),
    ClassData(
        title="Ekspeditionstid",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="UnitHours",
        uuid=uuids.UNIT_HOURS,
    ),
    ClassData(
        title="Telefontid",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="UnitPhoneHours",
        uuid=uuids.UNIT_PHONEHOURS,
    ),
    ClassData(
        title="Telefon",
        facet_user_key="org_unit_address_type",
        scope="PHONE",
        user_key="UnitPhone",
        uuid=uuids.UNIT_PHONE,
    ),
    ClassData(
        title="Fax",
        facet_user_key="org_unit_address_type",
        scope="PHONE",
        user_key="UnitFax",
        uuid=uuids.UNIT_FAX,
    ),
    ClassData(
        title="Email",
        facet_user_key="org_unit_address_type",
        scope="EMAIL",
        user_key="UnitEmail",
        uuid=uuids.UNIT_EMAIL,
    ),
    ClassData(
        title="Magkort",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="UnitMagID",
        uuid=uuids.UNIT_MAG_ID,
    ),
    ClassData(
        title="Alternativt navn",
        facet_user_key="org_unit_address_type",
        scope="TEXT",
        user_key="UnitNameAlt",
        uuid=uuids.UNIT_NAME_ALT,
    ),
    # Employee address types
    ClassData(
        title="Phone",  # "Telefon"?
        facet_user_key="employee_address_type",
        scope="PHONE",
        user_key="PhoneEmployee",
        uuid=uuids.PERSON_PHONE,
    ),
    ClassData(
        title="Email",
        facet_user_key="employee_address_type",
        scope="EMAIL",
        user_key="EmailEmployee",
        uuid=uuids.PERSON_EMAIL,
    ),
    ClassData(
        title="Lokale",
        facet_user_key="employee_address_type",
        scope="TEXT",
        user_key="RoomEmployee",
        uuid=uuids.PERSON_ROOM,
    ),
    # Engagements (?): primary and not primary
    ClassData(
        title="Primær",
        facet_user_key="primary_type",
        scope="100000",
        user_key="primary",
        uuid=uuids.PRIMARY,
    ),
    ClassData(
        title="Ikke-primær",
        facet_user_key="primary_type",
        scope="0",
        user_key="not_primary",
        uuid=uuids.NOT_PRIMARY,
    ),
    # Hierarchy names
    ClassData(
        title="Linjeorganisation",
        facet_user_key="org_unit_hierarchy",
        scope="TEXT",
        user_key="linjeorg",
        uuid=uuids.LINJE_ORG_HIERARCHY,
    ),
    ClassData(
        title="Sikkerhedsorganisation",
        facet_user_key="org_unit_hierarchy",
        scope="TEXT",
        user_key="sikkerhedsorg",
        uuid=uuids.SIKKERHEDS_ORG_HIERARCHY,
    ),
    # Miscellaneous placeholder (?) classes for predefined facets
    ClassData(
        title="Rolletype",
        facet_user_key="role_type",
        scope="TEXT",
        user_key="role_type",
        uuid="964c31a2-6267-4388-bff5-42d6f3c5f708",
    ),
    ClassData(
        title="Tilknytningsrolle",
        facet_user_key="association_type",
        scope="TEXT",
        user_key="association_type",
        uuid="ec534b86-3d9b-42d8-bff0-afc4f81719af",
    ),
    ClassData(
        title="Orlovstype",
        facet_user_key="leave_type",
        scope="TEXT",
        user_key="leave_type",
        uuid="d2892fa6-bc56-4c14-bd24-74ae0c71fa3a",
    ),
]


class LoraClass(LoraModelClient):
    @classmethod
    async def create(cls, data: ClassData):
        settings = config.get_config()
        client = cls(settings.mox_base)
        async with client.context():
            facet_uuid = await client._get_facet_uuid(data.facet_user_key)
            mox_class = Klasse.from_simplified_fields(
                facet_uuid=facet_uuid,
                uuid=data.uuid,
                user_key=data.user_key,
                organisation_uuid=uuids.ORG_UUID,
                title=data.title,
                scope=data.scope,
            )
            return await client.load_lora_objs([mox_class], disable_progressbar=True)

    async def _get_facet_uuid(self, facet_user_key: str):
        session: ClientSession = await self._verify_session()
        url = f"{self._base_url}/klassifikation/facet"
        async with session.get(url, params={"bvn": facet_user_key}) as response:
            resp_json = await response.json()
            return one(resp_json["results"][0])


class InitialDataImporter:
    async def run(self, last_import: datetime):
        """Perform all initial bootstrapping of OS2mo."""
        await self._import_organisation()
        await asyncio.gather(self._import_classes(), self._import_it_systems())

    async def _import_organisation(self):
        """Imports an organisation if missing, and adds all base facets"""
        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)
        try:
            await mox_helper.read_element_organisation_organisation(bvn="%")
        except ElementNotFound:
            print("No org found in LoRa. Performing initial setup.")
            importer = ImportHelper(
                create_defaults=True,
                mox_base=settings.mox_base,
                mora_base=settings.mora_base,
                store_integration_data=False,
                seperate_names=True,
            )
            importer.add_organisation(
                identifier="Århus Kommune",
                user_key="Århus Kommune",
                municipality_code=751,
                uuid=uuids.ORG_UUID,
            )
            # Perform initial import of org and facets
            importer.import_all()
        else:
            print("LoRa organisation already exists")

    async def _import_classes(self):
        """Import a set of predetermined classes. All the classes have
        predefined UUIDs which makes this function idempotent.
        """
        await asyncio.gather(*[LoraClass.create(cls) for cls in CLASSES])

    async def _import_it_systems(self):
        """Import predetermined IT systems. The UUID(s) are predefined which
        makes this function idempotent.
        """
        settings = config.get_config()
        mox_helper = await create_mox_helper(settings.mox_base)
        it_system = Itsystem(system_name="AZ", user_key="AZ")
        it_system.organisation_uuid = str(uuids.ORG_UUID)
        uuid = uuids.AZID_SYSTEM
        json = it_system.build()
        await mox_helper.insert_organisation_itsystem(json, str(uuid))
