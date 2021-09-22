import config
import mox_helpers.payloads as mox_payloads
import uuids
from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import ElementNotFound

from os2mo_data_import import ImportHelper  # type: ignore
from os2mo_data_import.mox_data_types import Itsystem

CLASSES = [
    (
        "Postadresse",
        "org_unit_address_type",
        "DAR",
        "AddressMailUnit",
        uuids.UNIT_POSTADDR,
    ),
    ("LOS ID", "org_unit_address_type", "TEXT", "LOSID", uuids.UNIT_LOS),
    ("CVR nummer", "org_unit_address_type", "TEXT", "CVRUnit", uuids.UNIT_CVR),
    ("EAN nummer", "org_unit_address_type", "EAN", "EANUnit", uuids.UNIT_EAN),
    (
        "P-nummer",
        "org_unit_address_type",
        "PNUMBER",
        "PNumber",
        uuids.UNIT_PNR,
    ),
    ("SE-nummer", "org_unit_address_type", "TEXT", "SENumber", uuids.UNIT_SENR),
    (
        "IntDebitor-Nr",
        "org_unit_address_type",
        "TEXT",
        "intdebit",
        uuids.UNIT_DEBITORNR,
    ),
    ("WWW", "org_unit_address_type", "WWW", "UnitWeb", uuids.UNIT_WWW),
    (
        "Ekspeditionstid",
        "org_unit_address_type",
        "TEXT",
        "UnitHours",
        uuids.UNIT_HOURS,
    ),
    (
        "Telefontid",
        "org_unit_address_type",
        "TEXT",
        "UnitPhoneHours",
        uuids.UNIT_PHONEHOURS,
    ),
    (
        "Telefon",
        "org_unit_address_type",
        "PHONE",
        "UnitPhone",
        uuids.UNIT_PHONE,
    ),
    ("Fax", "org_unit_address_type", "PHONE", "UnitFax", uuids.UNIT_FAX),
    ("Email", "org_unit_address_type", "EMAIL", "UnitEmail", uuids.UNIT_EMAIL),
    (
        "Magkort",
        "org_unit_address_type",
        "TEXT",
        "UnitMagID",
        uuids.UNIT_MAG_ID,
    ),
    (
        "Alternativt navn",
        "org_unit_address_type",
        "TEXT",
        "UnitNameAlt",
        uuids.UNIT_NAME_ALT,
    ),
    (
        "Phone",
        "employee_address_type",
        "PHONE",
        "PhoneEmployee",
        uuids.PERSON_PHONE,
    ),
    (
        "Email",
        "employee_address_type",
        "EMAIL",
        "EmailEmployee",
        uuids.PERSON_EMAIL,
    ),
    (
        "Lokale",
        "employee_address_type",
        "TEXT",
        "RoomEmployee",
        uuids.PERSON_ROOM,
    ),
    ("Primær", "primary_type", "100000", "primary", uuids.PRIMARY),
    ("Ikke-primær", "primary_type", "0", "not_primary", uuids.NOT_PRIMARY),
    (
        "Linjeorganisation",
        "org_unit_hierarchy",
        "TEXT",
        "linjeorg",
        uuids.LINJE_ORG_HIERARCHY,
    ),
    (
        "Sikkerhedsorganisation",
        "org_unit_hierarchy",
        "TEXT",
        "sikkerhedsorg",
        uuids.SIKKERHEDS_ORG_HIERARCHY,
    ),
    (
        "Rolletype",
        "role_type",
        "TEXT",
        "role_type",
        "964c31a2-6267-4388-bff5-42d6f3c5f708",
    ),
    (
        "Tilknytningsrolle",
        "association_type",
        "TEXT",
        "association_type",
        "ec534b86-3d9b-42d8-bff0-afc4f81719af",
    ),
    (
        "Orlovstype",
        "leave_type",
        "TEXT",
        "leave_type",
        "d2892fa6-bc56-4c14-bd24-74ae0c71fa3a",
    ),
    (
        "Alternativ stillingsbetegnelse",
        "employee_address_type",
        "TEXT",
        "AltJobTitle",
        uuids.PERSON_JOB_TITLE_ALT,
    ),
]


async def perform_initial_setup():
    """
    Perform all initial bootstrapping of OS2mo.
    Imports an organisation if missing, and adds all base facets
    Imports all pretedetermined classes and it systems
    """
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

    await import_remaining_classes()
    await import_it()


async def import_remaining_classes():
    """
    Import a set of predetermined classes. All the classes have predefined UUIDs
    which makes this function idempotent
    """
    settings = config.get_config()
    mox_helper = await create_mox_helper(settings.mox_base)

    for clazz in CLASSES:
        titel, facet, scope, bvn, uuid = clazz

        facet_uuid = await mox_helper.read_element_klassifikation_facet(bvn=facet)

        klasse = mox_payloads.lora_klasse(
            bvn=bvn,
            title=titel,
            facet_uuid=str(facet_uuid),
            org_uuid=str(uuids.ORG_UUID),
            scope=scope,
        )
        await mox_helper.insert_klassifikation_klasse(klasse, str(uuid))


async def import_it():
    """
    Import predetermined IT systems. The UUID(s) are predefined which makes this
    function idempotent.
    """
    settings = config.get_config()
    if settings.azid_it_system_uuid == uuids.AZID_SYSTEM:
        mox_helper = await create_mox_helper(settings.mox_base)
        it_system = Itsystem(system_name="AZ", user_key="AZ")
        it_system.organisation_uuid = str(uuids.ORG_UUID)
        uuid = uuids.AZID_SYSTEM
        json = it_system.build()
        await mox_helper.insert_organisation_itsystem(json, str(uuid))
    else:
        print(
            """Settings specify a non-default AZID IT system UUID, not creating
            default AZ IT system"""
        )
