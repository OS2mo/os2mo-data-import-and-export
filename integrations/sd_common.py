import os
import pickle
import hashlib
import requests
import xmltodict

INSTITUTION_IDENTIFIER = os.environ.get('INSTITUTION_IDENTIFIER')
SD_USER = os.environ.get('SD_USER', None)
SD_PASSWORD = os.environ.get('SD_PASSWORD', None)
if not (INSTITUTION_IDENTIFIER and SD_USER and SD_PASSWORD):
    raise Exception('Credentials missing')


def sd_lookup(url, params={}):
    BASE_URL = 'https://service.sd.dk/sdws/'
    full_url = BASE_URL + url

    payload = {
        'InstitutionIdentifier': INSTITUTION_IDENTIFIER,
    }
    payload.update(params)
    m = hashlib.sha256()
    m.update(str(sorted(payload)).encode())
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
    return xml_response
