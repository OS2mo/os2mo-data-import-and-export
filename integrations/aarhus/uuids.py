from ra_utils.generate_uuid import uuid_generator

uuid_gen = uuid_generator("aarhus-los")

AZID_SYSTEM = uuid_gen("AZID_SYSTEM")

PERSON_PHONE = uuid_gen("PERSON_PHONE")
PERSON_EMAIL = uuid_gen("PERSON_EMAIL")
PERSON_ROOM = uuid_gen("PERSON_ROOM")
PERSON_JOB_TITLE_ALT = uuid_gen("PERSON_JOB_TITLE_ALT")

UNIT_LOS = uuid_gen("UNIT_LOS")
UNIT_CVR = uuid_gen("UNIT_CVR")
UNIT_EAN = uuid_gen("UNIT_EAN")
UNIT_PNR = uuid_gen("UNIT_PNR")
UNIT_SENR = uuid_gen("UNIT_SENR")
UNIT_DEBITORNR = uuid_gen("UNIT_DEBITORNR")
UNIT_POSTADDR = uuid_gen("UNIT_POSTADDR")
UNIT_WWW = uuid_gen("UNIT_WWW")
UNIT_HOURS = uuid_gen("UNIT_HOURS")
UNIT_PHONEHOURS = uuid_gen("UNIT_PHONEHOURS")
UNIT_PHONE = uuid_gen("UNIT_PHONE")
UNIT_FAX = uuid_gen("UNIT_FAX")
UNIT_EMAIL = uuid_gen("UNIT_EMAIL")
UNIT_MAG_ID = uuid_gen("UNIT_MAG_ID")
UNIT_NAME_ALT = uuid_gen("UNIT_NAME_ALT")

LINJE_ORG_HIERARCHY = uuid_gen("LINJE_ORG_HIERARCHY")
SIKKERHEDS_ORG_HIERARCHY = uuid_gen("SIKKERHEDS_ORG_HIERARCHY")

NOT_PRIMARY = uuid_gen("NOT_PRIMARY")
PRIMARY = uuid_gen("PRIMARY")

ORG_UUID = uuid_gen("ORG_UUID")
