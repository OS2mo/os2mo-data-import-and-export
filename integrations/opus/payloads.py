# from collections import OrderedDict
from uuid import UUID

from integrations.opus import opus_helpers


def create_user(employee, org_uuid, uuid=None):
    if employee["firstName"] is None and employee["lastName"] is None:
        employee["firstName"] = "Ukendt"
        employee["lastName"] = "Ukendt"
        empl = dict(employee)
        empl.pop("cpr")
        print("No names on user")
        print(empl)
    cpr = opus_helpers.read_cpr(employee)
    payload = {
        "givenname": employee["firstName"],
        "surname": employee["lastName"],
        "cpr_no": cpr,
        "org": {"uuid": org_uuid},
    }
    if uuid is not None:
        payload["uuid"] = uuid
    return payload


def edit_engagement(data, mo_engagement_uuid):
    payload = {"type": "engagement", "uuid": mo_engagement_uuid, "data": data}
    return payload


def edit_role(validity, mo_role_uuid):
    data = {"validity": validity}
    payload = {"type": "role", "uuid": mo_role_uuid, "data": data}
    return payload


def create_engagement(
    employee, user_uuid, unit_uuid, job_function, engagement_type, primary, validity
):
    payload = {
        "type": "engagement",
        "org_unit": {"uuid": str(unit_uuid)},
        "person": {"uuid": user_uuid},
        "job_function": {"uuid": job_function},
        "primary": {"uuid": primary},
        "engagement_type": {"uuid": engagement_type},
        "user_key": employee["@id"],
        "validity": validity,
    }
    return payload


def create_role(employee, user_uuid, unit_uuid, role_type, validity):
    payload = {
        "type": "role",
        "org_unit": {"uuid": str(unit_uuid)},
        "person": {"uuid": user_uuid},
        "role_type": {"uuid": role_type},
        "user_key": "role_{}".format(employee["@id"]),
        "validity": validity,
    }
    return payload


def create_org_unit(unit, unit_user_key, unit_uuid, parent, unit_type, from_date):
    payload = {
        "uuid": unit_uuid,
        "user_key": unit_user_key,
        "name": unit["longName"],
        "parent": {"uuid": parent},
        "org_unit_type": {"uuid": unit_type},
        "validity": {"from": from_date, "to": None},
    }
    return payload


def edit_org_unit(unit, unit_user_key, unit_uuid, parent, unit_type, from_date):
    payload = {
        "type": "org_unit",
        "data": {
            "uuid": unit_uuid,
            "user_key": unit_user_key,
            "name": unit["longName"],
            "parent": {"uuid": parent},
            "org_unit_type": {"uuid": unit_type},
            "validity": {"from": from_date, "to": None},
        },
    }
    return payload


def terminate_detail(uuid, terminate_date, detail_type):
    payload = {"type": detail_type, "uuid": uuid, "validity": {"to": terminate_date}}
    return payload


def terminate_manager(uuid, terminate_date):
    payload = {"type": "manager", "uuid": uuid, "validity": {"to": terminate_date}}
    return payload


def connect_it_system_to_user(username, it_system, person_uuid, from_date):
    payload = {
        "type": "it",
        "user_key": username,
        "itsystem": {"uuid": it_system},
        "person": {"uuid": person_uuid},
        "validity": {"from": from_date, "to": None},
    }
    return payload


def edit_it_system_username(uuid, username, from_date):
    payload = {
        "type": "it",
        "uuid": uuid,
        "data": {
            "user_key": username,
            "validity": {"from": from_date, "to": None},
        },
    }
    return payload


def create_address(
    validity,
    address_type,
    value,
    unit_uuid=None,
    user_uuid=None,
    visibility: UUID = None,
):
    if unit_uuid is None and user_uuid is None:
        raise Exception("Either unit or user must be specified")
    if unit_uuid and user_uuid:
        raise Exception("Only a unit or a person can be specified")

    payload = {
        "type": "address",
        "value": value,
        "address_type": address_type,
        "validity": validity,
        "visibility": visibility,
    }

    if unit_uuid is not None:
        payload["org_unit"] = {"uuid": unit_uuid}
    if user_uuid is not None:
        payload["person"] = {"uuid": user_uuid}
    return payload


def edit_address(data, mo_address_uuid):
    payload = {"type": "address", "uuid": mo_address_uuid, "data": data}
    return payload


def create_manager(
    user_key, unit, person, manager_type, level, responsibility, validity
):
    payload = {
        "type": "manager",
        "user_key": user_key,
        "org_unit": {"uuid": unit},
        "person": {"uuid": person},
        "manager_type": {"uuid": manager_type},
        "manager_level": {"uuid": level},
        "responsibility": [  # OPUS will never give more than one
            {"uuid": responsibility}
        ],
        "validity": validity,
    }
    return payload


def edit_manager(
    object_uuid, unit, person, manager_type, level, responsibility, validity
):
    payload = {
        "type": "manager",
        "uuid": object_uuid,
        "data": {
            "org_unit": {"uuid": unit},
            "person": {"uuid": person},
            "manager_type": {"uuid": manager_type},
            "manager_level": {"uuid": level},
            "responsibility": [  # OPUS will never give more than one
                {"uuid": responsibility}
            ],
            "validity": validity,
        },
    }
    return payload
