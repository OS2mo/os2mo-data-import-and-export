def create_employee(*, uuid, givenname, surname, cpr_no):
    payload = {
        'uuid': uuid,
        'givenname': givenname,
        'surname': surname,
        'cpr_no': cpr_no
    }

    return payload


def create_engagement(*, uuid, org_unit_uuid, person_uuid, job_function_uuid,
                      engagement_type_uuid, from_date, to_date=None,
                      user_key=None):
    payload = {
        'type': 'engagement',
        'uuid': uuid,
        'org_unit': {
            'uuid': org_unit_uuid
        },
        'person': {
            'uuid': person_uuid
        },
        'job_function': {
            'uuid': job_function_uuid
        },
        'engagement_type': {
            'uuid': engagement_type_uuid
        },
        'validity': {
            'from': from_date,
            'to': to_date
        }

    }
    return [payload]


def edit_engagement(*, uuid, job_function_uuid, from_date, to_date):
    payload = {
        'type': 'engagement',
        'uuid': uuid,
        'data': {
            'job_function': {
                'uuid': job_function_uuid
            },
            'validity': {
                'from': from_date,
                'to': to_date
            }

        }
    }
    return [payload]


def create_org_unit(*, uuid, user_key, name, parent_uuid, org_unit_type_uuid,
                    from_date,
                    to_date):
    payload = {
        'type': 'org_unit',
        'uuid': uuid,
        'user_key': user_key,
        'name': name,
        'parent': {
            'uuid': parent_uuid
        },
        'org_unit_type': {
            'uuid': org_unit_type_uuid,
        },
        'validity': {
            'from': from_date,
            'to': to_date
        }

    }
    return payload


def edit_org_unit(*, uuid, user_key, name, org_unit_type_uuid, parent_uuid,
                  from_date,
                  to_date):
    payload = {
        'type': 'org_unit',
        'uuid': uuid,
        'data': {
            'uuid': uuid,
            'user_key': user_key,
            'name': name,
            'org_unit_type': {
                'uuid': org_unit_type_uuid
            },
            'parent': {
                'uuid': parent_uuid
            },
            'validity': {
                'from': from_date,
                'to': to_date
            }

        }
    }
    return [payload]


def create_address(*, uuid, value, address_type_uuid, person_uuid=None,
                   org_unit_uuid=None, from_date, to_date):
    payload = {
        'type': 'address',
        'uuid': uuid,
        'value': value,
        'address_type': {
            'uuid': address_type_uuid
        },
        'validity': {
            'from': from_date,
            'to': to_date
        }
    }
    if person_uuid:
        payload['person'] = {'uuid': person_uuid}
    if org_unit_uuid:
        payload['org_unit'] = {'uuid': org_unit_uuid}
    return [payload]


def edit_address(*, uuid, value, address_type_uuid, person_uuid=None,
                 org_unit_uuid=None, from_date, to_date):
    payload = {
        'type': 'address',
        'uuid': uuid,
        'data': {
            'value': value,
            'address_type': {
                'uuid': address_type_uuid
            },
            'validity': {
                'from': from_date,
                'to': to_date
            }
        }
    }

    if person_uuid:
        payload['data']['person'] = {'uuid': person_uuid}
    if org_unit_uuid:
        payload['data']['org_unit'] = {'uuid': org_unit_uuid}

    return [payload]


def create_it_rel(*, uuid, user_key, person_uuid, itsystem_uuid, from_date,
                  to_date=None):
    payload = {
        'type': 'it',
        'uuid': uuid,
        'user_key': user_key,
        'person': {
            'uuid': person_uuid
        },
        'itsystem': {
            'uuid': itsystem_uuid
        },
        'validity': {
            'from': from_date,
            'to': to_date
        }

    }
    return [payload]


def edit_it_rel(*, uuid, user_key, person_uuid, itsystem_uuid, from_date,
                to_date=None):
    payload = {
        'type': 'it',
        'uuid': uuid,
        'data': {
            'user_key': user_key,
            'person': {
                'uuid': person_uuid
            },
            'itsystem': {
                'uuid': itsystem_uuid
            },
            'validity': {
                'from': from_date,
                'to': to_date
            }
        }
    }
    return [payload]
