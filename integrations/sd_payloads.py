def new_department(department, org, unit_type):
    payload = {
        'uuid': department['DepartmentUUIDIdentifier'],
        'user_key': department['DepartmentIdentifier'],
        'name': 'Unnamed department',
        'parent': {'uuid': org},
        'org_unit_type': {'uuid': unit_type},
        'validity': {
            'from': '1900-01-01',
            'to': None
        }
    }
    return payload

def engagement(data, mo_engagement):
    payload = {
        'type': 'engagement',
        'uuid': mo_engagement['uuid'],
        'data': data
    }
    return payload



def create_leave(mo_eng, mo_person, leave_uuid, job_id, validity):
    payload = {
        'type': 'leave',
        'org_unit': {'uuid': mo_eng['org_unit']['uuid']},
        'person': {'uuid': mo_person['uuid']},
        'leave_type': {'uuid': leave_uuid},
        'user_key': job_id,
        'validity': validity
    }
    return payload

def create_engagement(org_unit, mo_person, job_function, engagement_type,
                      job_id, engagement_info, validity):
    working_time = float(engagement_info['working_time'][0]['OccupationRate'])
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

def create_association(org_unit, mo_person, association_uuid, job_id, validity):
    payload = {
        'type': 'association',
        'org_unit': {'uuid': org_unit},
        'person': {'uuid': mo_person['uuid']},
        'association_type': {'uuid': association_uuid},
        'user_key': job_id,
        'validity': validity
    }
    return payload


def profession(profession, org, job_function_facet):
    validity = {
        'from': '1900-01-01',
        'to': 'infinity'
    }

    # "integrationsdata":  # TODO: Check this
    properties = {
        'brugervendtnoegle': profession,
        'titel':  profession,
        'omfang': 'TEXT',
        "virkning": validity
    }
    attributter = {
        'klasseegenskaber': [properties]
    }
    relationer = {
        'ansvarlig': [
            {
                'objekttype': 'organisation',
                'uuid': org,
                'virkning': validity
            }
        ],
        'facet': [
            {
                'objekttype': 'facet',
                'uuid': job_function_facet,
                'virkning': validity
            }
        ]
    }
    tilstande = {
        'klassepubliceret': [
            {
                'publiceret': 'Publiceret',
                'virkning': validity
            }
        ]
    }

    payload = {
        "attributter": attributter,
        "relationer": relationer,
        "tilstande": tilstande
    }
    return payload
