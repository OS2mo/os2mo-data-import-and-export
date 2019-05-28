import os
import pickle
import logging
import hashlib
import requests
import xmltodict

logger = logging.getLogger("sdCommon")

INSTITUTION_IDENTIFIER = os.environ.get('INSTITUTION_IDENTIFIER')
SD_USER = os.environ.get('SD_USER', None)
SD_PASSWORD = os.environ.get('SD_PASSWORD', None)
if not (INSTITUTION_IDENTIFIER and SD_USER and SD_PASSWORD):
    raise Exception('Credentials missing')


def sd_lookup(url, params={}):
    logger.info('Retrive: {}'.format(url))
    logger.debug('Params: {}'.format(params))

    BASE_URL = 'https://service.sd.dk/sdws/'
    full_url = BASE_URL + url

    payload = {
        'InstitutionIdentifier': INSTITUTION_IDENTIFIER,
    }
    payload.update(params)
    m = hashlib.sha256()

    keys = sorted(payload.keys())
    for key in keys:
        m.update((str(key) + str(payload[key])).encode())
    m.update(full_url.encode())
    lookup_id = m.hexdigest()

    try:
        with open(lookup_id + '.p', 'rb') as f:
            response = pickle.load(f)
        print('CACHED')
    except FileNotFoundError:
        response = requests.get(
            full_url,
            params=payload,
            auth=(SD_USER, SD_PASSWORD)
        )
        with open(lookup_id + '.p', 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    xml_response = xmltodict.parse(response.text)[url]
    logger.debug('Done with {}'.format(url))
    return xml_response


def calc_employment_id(employment):
    employment_id = employment['EmploymentIdentifier']
    try:
        employment_number = int(employment_id)
    except ValueError:  # Job id is not a number?
        employment_number = 999999

    employment_id = {
        'id': employment_id,
        'value': employment_number
    }
    return employment_id
