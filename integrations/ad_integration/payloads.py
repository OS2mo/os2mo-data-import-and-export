from typing import Optional


def create_user(cpr: str, ad_user: dict, org_uuid, uuid: Optional[str] = None) -> dict:
    payload = {
        "cpr_no": cpr,
        "uuid": uuid or ad_user["ObjectGUID"],
        "givenname": ad_user["GivenName"],
        "surname": ad_user["Surname"],
        "org": {"uuid": org_uuid},
    }
    return payload


def connect_it_system_to_user(user_uuid, username, it_system, from_date="1930-01-01"):
    payload = {
        "type": "it",
        "user_key": username,
        "itsystem": {"uuid": it_system},
        "person": {"uuid": user_uuid},
        "validity": {"from": from_date, "to": None},
    }
    return payload


# almost all parameters could be replaced with direct reading from settings
def create_engagement(
    ad_user, unit_uuid, person_uuid, job_function, engagement_type, validity
):
    payload = {
        "type": "engagement",
        "org_unit": {"uuid": str(unit_uuid)},
        "person": {"uuid": str(person_uuid)},
        "job_function": {"uuid": job_function},
        "engagement_type": {"uuid": engagement_type},
        "user_key": ad_user["SamAccountName"],
        "validity": validity,
    }
    return payload


def create_unit(uuid, unit_name, unit_type, parent):
    payload = {
        "uuid": uuid,
        "user_key": unit_name,
        "name": unit_name,
        "parent": {"uuid": parent},
        "org_unit_type": {"uuid": unit_type},
        "validity": {"from": "1930-01-01", "to": None},
    }
    return payload


def terminate_engagement(uuid, terminate_date):
    payload = {
        "type": "engagement",
        "uuid": uuid,
        "validity": {"to": terminate_date.strftime("%Y-%m-%d")},
    }
    return payload


# Same code is found in sd_payloads and opus_payloads
def klasse(bvn, navn, org, facet_uuid):
    validity = {"from": "1930-01-01", "to": "infinity"}

    # "integrationsdata":
    properties = {
        "brugervendtnoegle": bvn,
        "titel": navn,
        "omfang": "TEXT",
        "virkning": validity,
    }
    attributter = {"klasseegenskaber": [properties]}
    relationer = {
        "ansvarlig": [
            {"objekttype": "organisation", "uuid": org, "virkning": validity}
        ],
        "facet": [{"objekttype": "facet", "uuid": facet_uuid, "virkning": validity}],
    }
    tilstande = {
        "klassepubliceret": [{"publiceret": "Publiceret", "virkning": validity}]
    }

    payload = {
        "attributter": attributter,
        "relationer": relationer,
        "tilstande": tilstande,
    }
    return payload
