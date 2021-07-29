import mox_helpers.payloads as mox_payloads
from mox_helpers import mox_util
from mox_helpers.mox_helper import ElementNotFound
from os2mo_data_import import ImportHelper
from os2mo_data_import.mox_data_types import Itsystem

import config
import uuids

UNIT_DAR = (
    "Postadresse",
    "org_unit_address_type",
    "DAR",
    "AddressMailUnit",
    uuids.UNIT_POSTADDR,
)
UNIT_LOSID = ("LOS ID", "org_unit_address_type", "TEXT", "LOSID", uuids.UNIT_LOS)
UNIT_CVR = ("CVR nummer", "org_unit_address_type", "TEXT", "CVRUnit", uuids.UNIT_CVR)
UNIT_EAN = ("EAN nummer", "org_unit_address_type", "EAN", "EANUnit", uuids.UNIT_EAN)
UNIT_PNUMBER = (
    "P-nummer",
    "org_unit_address_type",
    "PNUMBER",
    "PNumber",
    uuids.UNIT_PNR,
)
UNIT_SE = ("SE-nummer", "org_unit_address_type", "TEXT", "SENumber", uuids.UNIT_SENR)
UNIT_INTDEBIT = (
    "IntDebitor-Nr",
    "org_unit_address_type",
    "TEXT",
    "intdebit",
    uuids.UNIT_DEBITORNR,
)
UNIT_WWW = ("WWW", "org_unit_address_type", "WWW", "UnitWeb", uuids.UNIT_WWW)
UNIT_HOURS = (
    "Ekspeditionstid",
    "org_unit_address_type",
    "TEXT",
    "UnitHours",
    uuids.UNIT_HOURS,
)
UNIT_PHONEHOURS = (
    "Telefontid",
    "org_unit_address_type",
    "TEXT",
    "UnitPhoneHours",
    uuids.UNIT_PHONEHOURS,
)
UNIT_PHONE = (
    "Telefon",
    "org_unit_address_type",
    "PHONE",
    "UnitPhone",
    uuids.UNIT_PHONE,
)
UNIT_FAX = ("Fax", "org_unit_address_type", "PHONE", "UnitFax", uuids.UNIT_FAX)
UNIT_EMAIL = ("Email", "org_unit_address_type", "EMAIL", "UnitEmail", uuids.UNIT_EMAIL)
UNIT_MAG_ID = (
    "Magkort",
    "org_unit_address_type",
    "TEXT",
    "UnitMagID",
    uuids.UNIT_MAG_ID,
)
UNIT_NAME_ALT = (
    "Alternativt navn",
    "org_unit_address_type",
    "TEXT",
    "UnitNameAlt",
    uuids.UNIT_NAME_ALT,
)

PERSON_PHONE = (
    "Phone",
    "employee_address_type",
    "PHONE",
    "PhoneEmployee",
    uuids.PERSON_PHONE,
)
PERSON_EMAIL = (
    "Email",
    "employee_address_type",
    "EMAIL",
    "EmailEmployee",
    uuids.PERSON_EMAIL,
)
PERSON_ROOM = (
    "Lokale",
    "employee_address_type",
    "TEXT",
    "RoomEmployee",
    uuids.PERSON_ROOM,
)

PRIMARY = ("Primær", "primary_type", "100000", "primary", uuids.PRIMARY)
NOT_PRIMARY = ("Ikke-primær", "primary_type", "0", "not_primary", uuids.NOT_PRIMARY)

LINJE_ORG = (
    "Linjeorganisation",
    "org_unit_hierarchy",
    "TEXT",
    "linjeorg",
    uuids.LINJE_ORG_HIERARCHY,
)
SIKKERHEDS_ORG = (
    "Sikkerhedsorganisation",
    "org_unit_hierarchy",
    "TEXT",
    "sikkerhedsorg",
    uuids.SIKKERHEDS_ORG_HIERARCHY,
)

DUMMY_ROLE_TYPE = (
    "Rolletype",
    "role_type",
    "TEXT",
    "role_type",
    "964c31a2-6267-4388-bff5-42d6f3c5f708",
)
DUMMY_ASSOCIATION_TYPE = (
    "Tilknytningsrolle",
    "association_type",
    "TEXT",
    "association_type",
    "ec534b86-3d9b-42d8-bff0-afc4f81719af",
)
DUMMY_MANAGER_TYPE = (
    "Ledertype",
    "manager_type",
    "TEXT",
    "manager_type",
    "f7ba7957-3859-44c2-b21f-8e53b3f0019b",
)
DUMMY_RESPONSIBILITY = (
    "Lederansvar",
    "responsibility",
    "TEXT",
    "responsibility",
    "bfd7cbf9-4c13-48d5-890e-c2620cbce411",
)
DUMMY_MANAGER_LEVEL = (
    "Lederniveau",
    "manager_level",
    "TEXT",
    "manager_level",
    "e1c1dbcf-80f0-4a82-9a44-d06f2001be2b",
)
DUMMY_LEAVE_TYPE = (
    "Orlovstype",
    "leave_type",
    "TEXT",
    "leave_type",
    "d2892fa6-bc56-4c14-bd24-74ae0c71fa3a",
)


async def perform_initial_setup():
    """
    Perform all initial bootstrapping of OS2mo.
    Imports an organisation if missing, and adds all base facets
    Imports all pretedetermined classes and it systems
    """
    settings = config.get_config()
    mox_helper = await mox_util.create_mox_helper(settings.mox_base)
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

    await import_remaining_classes()
    await import_it()


async def import_remaining_classes():
    """
    Import a set of predetermined classes. All the classes have predefined UUIDs
    which makes this function idempotent
    """
    mox_helper = await mox_util.create_mox_helper("http://localhost:8080")

    for clazz in [
        UNIT_DAR,
        UNIT_EAN,
        UNIT_LOSID,
        UNIT_SE,
        UNIT_CVR,
        UNIT_PNUMBER,
        UNIT_INTDEBIT,
        UNIT_WWW,
        UNIT_HOURS,
        UNIT_PHONEHOURS,
        UNIT_PHONE,
        UNIT_FAX,
        UNIT_EMAIL,
        UNIT_MAG_ID,
        UNIT_NAME_ALT,
        PERSON_EMAIL,
        PERSON_PHONE,
        PERSON_ROOM,
        PRIMARY,
        NOT_PRIMARY,
        LINJE_ORG,
        SIKKERHEDS_ORG,
        DUMMY_ROLE_TYPE,
        DUMMY_ASSOCIATION_TYPE,
        DUMMY_LEAVE_TYPE,
        DUMMY_MANAGER_TYPE,
        DUMMY_RESPONSIBILITY,
        DUMMY_MANAGER_LEVEL,
    ]:
        titel, facet, scope, bvn, uuid = clazz

        facet_uuid = await mox_helper.read_element_klassifikation_facet(bvn=facet)

        klasse = mox_payloads.lora_klasse(
            bvn=bvn,
            title=titel,
            facet_uuid=facet_uuid,
            org_uuid=uuids.ORG_UUID,
            scope=scope,
        )
        await mox_helper.insert_klassifikation_klasse(klasse, uuid)


async def import_it():
    """
    Import predetermined IT systems. The UUID(s) are predefined which makes this
    function idempotent.
    """
    settings = config.get_config()
    mox_helper = await mox_util.create_mox_helper(settings.mox_base)
    it_system = Itsystem(
        system_name="AZ",
        user_key="AZ",
    )
    it_system.organisation_uuid = uuids.ORG_UUID
    uuid = uuids.AZID_SYSTEM

    json = it_system.build()
    await mox_helper.insert_organisation_itsystem(json, uuid)
