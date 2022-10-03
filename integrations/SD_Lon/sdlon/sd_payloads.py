from mox_helpers.payloads import lora_klasse


def create_org_unit(department, name, org, unit_type, from_date):
    payload = {
        "uuid": department["DepartmentUUIDIdentifier"],
        "user_key": department["DepartmentIdentifier"],
        "name": name,
        "parent": {
            "uuid": department["DepartmentReference"]["DepartmentUUIDIdentifier"]
        },
        "org_unit_type": {"uuid": unit_type},
        "validity": {"from": from_date, "to": None},
    }
    return payload


def create_single_org_unit(department, unit_type, unit_level, parent):
    payload = {
        "uuid": department["DepartmentUUIDIdentifier"],
        "user_key": department["DepartmentIdentifier"],
        "name": department["DepartmentName"],
        "parent": {"uuid": parent},
        "org_unit_type": {"uuid": unit_type},
        "org_unit_level": {"uuid": unit_level},
        "validity": {"from": department["ActivationDate"], "to": None},
    }
    return payload


def edit_org_unit(
    user_key, name, unit_uuid, parent, ou_level, ou_type, from_date, to_date=None
):
    payload = {
        "type": "org_unit",
        "data": {
            "uuid": unit_uuid,
            "user_key": user_key,
            "name": name,
            "parent": {"uuid": parent},
            "org_unit_level": {"uuid": ou_level},
            "org_unit_type": {"uuid": ou_type},
            "validity": {"from": from_date, "to": to_date},
        },
    }
    return payload


def engagement(data, mo_engagement):
    payload = {"type": "engagement", "uuid": mo_engagement["uuid"], "data": data}
    return payload


def association(data, uuid):
    payload = {"type": "association", "uuid": uuid, "data": data}
    return payload


def create_leave(mo_eng, person_uuid, leave_uuid, job_id, validity):
    payload = {
        "type": "leave",
        "engagement": {"uuid": mo_eng["uuid"]},
        "person": {"uuid": person_uuid},
        "leave_type": {"uuid": leave_uuid},
        "user_key": job_id,
        "validity": validity,
    }
    return payload


def create_engagement(
    org_unit,
    person_uuid,
    job_function,
    engagement_type,
    primary,
    user_key,
    engagement_info,
    validity,
    **extensions
):
    try:
        working_time = float(engagement_info["working_time"][0]["OccupationRate"])
    except IndexError:
        working_time = 0
    payload = {
        "type": "engagement",
        "org_unit": {"uuid": org_unit},
        "person": {"uuid": person_uuid},
        "job_function": {"uuid": job_function},
        "primary": {"uuid": primary},
        "engagement_type": {"uuid": engagement_type},
        "user_key": user_key,
        "fraction": int(working_time * 1000000),
        "validity": validity,
    }
    payload.update(extensions)
    return payload


def create_association(org_unit, person_uuid, association_uuid, job_id, validity):
    payload = {
        "type": "association",
        "org_unit": {"uuid": org_unit},
        "person": {"uuid": person_uuid},
        "association_type": {"uuid": association_uuid},
        "user_key": job_id,
        "validity": validity,
    }
    return payload


def connect_it_system_to_user(username, it_system, person_uuid):
    payload = {
        "type": "it",
        "user_key": username,
        "itsystem": {"uuid": it_system},
        "person": {"uuid": person_uuid},
        "validity": {"from": "1930-01-01", "to": None},
    }
    return payload


def edit_klasse_title(titel):
    payload = {
        "attributter": {
            "klasseegenskaber": [
                {
                    "titel": titel,
                    "virkning": {
                        "from": "1930-01-01",
                        "to": "infinity",
                        "aktoerref": "ddc99abd-c1b0-48c2-aef7-74fea841adae",
                        "aktoertypekode": "Bruger",
                    },
                }
            ]
        }
    }
    return payload


def profession(profession, org, job_function_facet):
    klasse = lora_klasse(
        bvn=profession,
        title=profession,
        facet_uuid=job_function_facet,
        org_uuid=org,
        scope="TEXT",
        dato="1930-01-01",
    )
    return klasse


def engagement_type(engagement_type_ref, job_position_id, org, engagement_type_facet):
    klasse = lora_klasse(
        bvn=engagement_type_ref,
        title=job_position_id,
        facet_uuid=engagement_type_facet,
        org_uuid=org,
        scope="TEXT",
        dato="1930-01-01",
    )
    return klasse
