from datetime import datetime
from typing import Optional
from uuid import UUID


def create_employee(*, uuid: UUID, name, cpr_no):
    payload = {"type": "employee", "uuid": str(uuid), "name": name, "cpr_no": cpr_no}

    return payload


def create_engagement(
    *,
    uuid: UUID,
    org_unit_uuid: UUID,
    person_uuid: UUID,
    job_function_uuid: UUID,
    engagement_type_uuid: UUID,
    from_date: str,
    to_date: Optional[str],
    primary_uuid: UUID,
    user_key=str,
):
    payload = {
        "type": "engagement",
        "uuid": str(uuid),
        "org_unit": {"uuid": str(org_unit_uuid)},
        "person": {"uuid": str(person_uuid)},
        "job_function": {"uuid": str(job_function_uuid)},
        "primary": {"uuid": str(primary_uuid)},
        "engagement_type": {"uuid": str(engagement_type_uuid)},
        "user_key": user_key,
        "validity": {
            "from": from_date,
            "to": to_date,
        },
    }
    return payload


def create_org_unit(
    *,
    uuid: UUID,
    user_key: str,
    name: Optional[str],
    parent_uuid: UUID,
    org_unit_hierarchy: Optional[UUID],
    org_unit_type_uuid: Optional[UUID],
    from_date: str,
    to_date: Optional[str],
):
    payload = {
        "type": "org_unit",
        "uuid": str(uuid),
        "user_key": user_key,
        "parent": {"uuid": str(parent_uuid)},
        "validity": {"from": from_date, "to": to_date},
    }
    if name:
        payload["name"] = name
    if org_unit_hierarchy:
        payload["org_unit_hierarchy"] = {"uuid": str(org_unit_hierarchy)}
    if org_unit_type_uuid:
        payload["org_unit_type"] = {"uuid": str(org_unit_type_uuid)}
    return payload


def create_address(
    *,
    uuid: UUID,
    value: Optional[str],
    address_type_uuid: UUID,
    person_uuid: UUID = None,
    org_unit_uuid: UUID = None,
    from_date: str,
    to_date: Optional[str],
):
    payload = {
        "type": "address",
        "uuid": str(uuid),
        "value": value,
        "address_type": {"uuid": str(address_type_uuid)},
        "validity": {"from": from_date, "to": to_date},
    }
    if person_uuid:
        payload["person"] = {"uuid": str(person_uuid)}
    if org_unit_uuid:
        payload["org_unit"] = {"uuid": str(org_unit_uuid)}
    return payload


def create_it_rel(
    *,
    uuid: Optional[UUID],
    user_key: Optional[str],
    person_uuid: UUID,
    itsystem_uuid: Optional[UUID],
    from_date: str,
    to_date: str = None,
):
    payload = {
        "type": "it",
        "uuid": str(uuid),
        "user_key": user_key,
        "person": {"uuid": str(person_uuid)},
        "itsystem": {"uuid": str(itsystem_uuid)},
        "validity": {"from": from_date, "to": to_date},
    }
    return payload


def create_manager(
    *,
    uuid: UUID,
    person_uuid: UUID,
    org_unit_uuid: UUID,
    manager_type_uuid: UUID,
    manager_level_uuid: UUID,
    responsibility_uuid: UUID,
    from_date: str,
    to_date: str = None,
):
    payload = {
        "type": "manager",
        "uuid": str(uuid),
        "person": {"uuid": str(person_uuid)},
        "org_unit": {"uuid": str(org_unit_uuid)},
        "manager_type": {"uuid": str(manager_type_uuid)},
        "manager_level": {"uuid": str(manager_level_uuid)},
        "responsibility": [{"uuid": str(responsibility_uuid)}],
        "validity": {"from": from_date, "to": to_date},
    }

    return payload


def convert_create_to_edit(payload: dict, from_date: str = None):
    """Convert an existing create payload to an edit payload"""
    edit_payload = {
        "data": {**payload},
        "uuid": payload["uuid"],
        "type": payload["type"],
    }
    if from_date:
        edit_payload["data"]["validity"] = {"from": from_date}

    return edit_payload


def terminate_detail(type: str, uuid: UUID, to_date: datetime):
    """Create payload for terminating a MO detail"""
    payload = {
        "type": type,
        "uuid": str(uuid),
        "validity": {"to": to_date.date().isoformat()},
    }
    return payload
