def create_user(employee, org_uuid, uuid=None):
    payload = {
        'givenname': employee['firstName'],
        'surname': employee['lastName'],
        'cpr_no': employee['cpr']['#text'],
        'org': {
            'uuid': org_uuid
        }
    }
    if uuid is not None:
        payload['uuid'] = uuid
    return payload


def edit_engagement(data, mo_engagement_uuid):
    payload = {
        'type': 'engagement',
        'uuid': mo_engagement_uuid,
        'data': data
    }
    return payload


def create_engagement(org_unit, mo_person, job_function, engagement_type,
                      job_id, engagement_info, validity):
    try:
        working_time = float(engagement_info['working_time'][0]['OccupationRate'])
    except IndexError:
        working_time = 0
    payload = {
        'type': 'engagement',
        'org_unit': {'uuid': org_unit},
        'person': {'uuid': mo_person['uuid']},
        'job_function': {'uuid': job_function},
        'engagement_type': {'uuid': engagement_type},
        'user_key': job_id,
        'fraction': int(working_time * 1000000),
        'validity': validity
    }
    return payload


def connect_it_system_to_user(username, it_system, person_uuid):
    payload = {
        'type': 'it',
        'user_key': username,
        'it-system': {'uuid': it_system},
        'person': {'uuid': person_uuid},
        'validity': {
            'from': None,
            'to': None
        }
    }
    return payload
