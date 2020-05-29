def edit_org_unit(user_key, name, unit_uuid, parent, ou_type, from_date, to_date):
    payload = {
        'type': 'org_unit',
        'data': {
            'uuid': unit_uuid,
            'user_key': user_key,
            'name': name,
            'parent': {
                'uuid': parent
            },
            'org_unit_type': {
                'uuid': ou_type
            },
            'validity': {
                'from': from_date,
                'to': to_date
            }

        }
    }
    return payload


def edit_address(data, mo_address_uuid):
    payload = {
        'type': 'address',
        'uuid': mo_address_uuid,
        'data': data
    }
    return payload
