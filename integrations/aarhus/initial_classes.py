from dataclasses import dataclass
from uuid import UUID

import uuids


@dataclass
class Class:
    titel: str
    facet: str
    scope: str
    bvn: str
    uuid: UUID


CLASSES = [
    Class(
        "Postadresse",
        "org_unit_address_type",
        "DAR",
        "AddressMailUnit",
        uuids.UNIT_POSTADDR,
    ),
    Class("LOS ID", "org_unit_address_type", "TEXT", "LOSID", uuids.UNIT_LOS),
    Class("CVR nummer", "org_unit_address_type", "TEXT", "CVRUnit", uuids.UNIT_CVR),
    Class("EAN nummer", "org_unit_address_type", "EAN", "EANUnit", uuids.UNIT_EAN),
    Class(
        "P-nummer",
        "org_unit_address_type",
        "PNUMBER",
        "PNumber",
        uuids.UNIT_PNR,
    ),
    Class("SE-nummer", "org_unit_address_type", "TEXT", "SENumber", uuids.UNIT_SENR),
    Class(
        "IntDebitor-Nr",
        "org_unit_address_type",
        "TEXT",
        "intdebit",
        uuids.UNIT_DEBITORNR,
    ),
    Class("WWW", "org_unit_address_type", "WWW", "UnitWeb", uuids.UNIT_WWW),
    Class(
        "Ekspeditionstid",
        "org_unit_address_type",
        "TEXT",
        "UnitHours",
        uuids.UNIT_HOURS,
    ),
    Class(
        "Telefontid",
        "org_unit_address_type",
        "TEXT",
        "UnitPhoneHours",
        uuids.UNIT_PHONEHOURS,
    ),
    Class(
        "Telefon",
        "org_unit_address_type",
        "PHONE",
        "UnitPhone",
        uuids.UNIT_PHONE,
    ),
    Class("Fax", "org_unit_address_type", "PHONE", "UnitFax", uuids.UNIT_FAX),
    Class("Email", "org_unit_address_type", "EMAIL", "UnitEmail", uuids.UNIT_EMAIL),
    Class(
        "Magkort",
        "org_unit_address_type",
        "TEXT",
        "UnitMagID",
        uuids.UNIT_MAG_ID,
    ),
    Class(
        "Alternativt navn",
        "org_unit_address_type",
        "TEXT",
        "UnitNameAlt",
        uuids.UNIT_NAME_ALT,
    ),
    Class(
        "Phone",
        "employee_address_type",
        "PHONE",
        "PhoneEmployee",
        uuids.PERSON_PHONE,
    ),
    Class(
        "Email",
        "employee_address_type",
        "EMAIL",
        "EmailEmployee",
        uuids.PERSON_EMAIL,
    ),
    Class(
        "Lokale",
        "employee_address_type",
        "TEXT",
        "RoomEmployee",
        uuids.PERSON_ROOM,
    ),
    Class("Primær", "primary_type", "100000", "primary", uuids.PRIMARY),
    Class("Ikke-primær", "primary_type", "0", "non-primary", uuids.NOT_PRIMARY),
    Class(
        "Linjeorganisation",
        "org_unit_hierarchy",
        "TEXT",
        "linjeorg",
        uuids.LINJE_ORG_HIERARCHY,
    ),
    Class(
        "Sikkerhedsorganisation",
        "org_unit_hierarchy",
        "TEXT",
        "sikkerhedsorg",
        uuids.SIKKERHEDS_ORG_HIERARCHY,
    ),
    Class(
        "Rolletype",
        "role_type",
        "TEXT",
        "role_type",
        UUID("964c31a2-6267-4388-bff5-42d6f3c5f708"),
    ),
    Class(
        "Tilknytningsrolle",
        "association_type",
        "TEXT",
        "association_type",
        UUID("ec534b86-3d9b-42d8-bff0-afc4f81719af"),
    ),
    Class(
        "Orlovstype",
        "leave_type",
        "TEXT",
        "leave_type",
        UUID("d2892fa6-bc56-4c14-bd24-74ae0c71fa3a"),
    ),
    Class(
        "Alternativ stillingsbetegnelse",
        "employee_address_type",
        "TEXT",
        "AltJobTitle",
        uuids.PERSON_JOB_TITLE_ALT,
    ),
]
