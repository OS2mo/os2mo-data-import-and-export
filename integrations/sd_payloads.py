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
