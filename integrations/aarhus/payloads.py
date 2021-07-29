def create_employee(*, uuid, name, cpr_no):
    payload = {"type": "employee", "uuid": uuid, "name": name, "cpr_no": cpr_no}

    return payload


def create_engagement(
    *,
    uuid: str,
    org_unit_uuid: str,
    person_uuid: str,
    job_function_uuid: str,
    engagement_type_uuid: str,
    from_date: str,
    to_date: str,
    primary_uuid=str,
    user_key=str,
):
    payload = {
        "type": "engagement",
        "uuid": uuid,
        "org_unit": {"uuid": org_unit_uuid},
        "person": {"uuid": person_uuid},
        "job_function": {"uuid": job_function_uuid},
        "primary": {"uuid": primary_uuid},
        "engagement_type": {"uuid": engagement_type_uuid},
        "user_key": user_key,
        "validity": {
            "from": from_date,
            "to": to_date,
        },
    }
    return payload


def create_org_unit(
    *,
    uuid: str,
    user_key: str,
    name: str,
    parent_uuid: str,
    org_unit_hierarchy: str,
    org_unit_type_uuid: str,
    from_date: str,
    to_date: str,
):
    payload = {
        "type": "org_unit",
        "uuid": uuid,
        "user_key": user_key,
        "parent": {"uuid": parent_uuid},
        "validity": {"from": from_date, "to": to_date},
    }
    if name:
        payload["name"] = name
    if org_unit_hierarchy:
        payload["org_unit_hierarchy"] = {"uuid": org_unit_hierarchy}
    if org_unit_type_uuid:
        payload["org_unit_type"] = {"uuid": org_unit_type_uuid}
    return payload


def create_address(
    *,
    uuid: str,
    value: str,
    address_type_uuid: str,
    person_uuid: str = None,
    org_unit_uuid: str = None,
    from_date: str,
    to_date: str,
):
    payload = {
        "type": "address",
        "uuid": uuid,
        "value": value,
        "address_type": {"uuid": address_type_uuid},
        "validity": {"from": from_date, "to": to_date},
    }
    if person_uuid:
        payload["person"] = {"uuid": person_uuid}
    if org_unit_uuid:
        payload["org_unit"] = {"uuid": org_unit_uuid}
    return payload


def create_it_rel(
    *,
    uuid: str,
    user_key: str,
    person_uuid: str,
    itsystem_uuid: str,
    from_date: str,
    to_date: str = None,
):
    payload = {
        "type": "it",
        "uuid": uuid,
        "user_key": user_key,
        "person": {"uuid": person_uuid},
        "itsystem": {"uuid": itsystem_uuid},
        "validity": {"from": from_date, "to": to_date},
    }
    return payload


def create_manager(
    *,
    uuid,
    person_uuid,
    org_unit_uuid,
    manager_type_uuid,
    manager_level_uuid,
    responsibility_uuid,
    from_date: str,
    to_date: str = None,
):
    payload = {
        "type": "manager",
        "uuid": uuid,
        "person": {"uuid": person_uuid},
        "org_unit": {"uuid": org_unit_uuid},
        "manager_type": {"uuid": manager_type_uuid},
        "manager_level": {"uuid": manager_level_uuid},
        "responsibility": [{"uuid": responsibility_uuid}],
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
