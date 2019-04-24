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
