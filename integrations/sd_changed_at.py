import os
import pickle
import requests
import xmltodict
import datetime

INSTITUTION_IDENTIFIER = os.environ.get('INSTITUTION_IDENTIFIER')
SD_USER = os.environ.get('SD_USER', None)
SD_PASSWORD = os.environ.get('SD_PASSWORD', None)
if not (INSTITUTION_IDENTIFIER and SD_USER and SD_PASSWORD):
    raise Exception('Credentials missing')

BASE_URL = 'https://service.sd.dk/sdws/'
# BASE_URL = 'http://localhost/'
SD_CHANGES = [
    'GetPersonChangedAtDate20111201',
    'GetEmploymentChangedAtDate20111201'
]

from_date = datetime.datetime(2018, 9, 6, 0, 0)
to_date = datetime.datetime(2018, 9, 16, 0, 0)


def _sd_lookup(url):
    payload = {
        'InstitutionIdentifier': INSTITUTION_IDENTIFIER,
        'ActivationDate': from_date.strftime('%d.%m.%Y'),
        'DeactivationDate': to_date.strftime('%d.%m.%Y')
    }

    full_url = BASE_URL + url
    url_id = url
    url_id += 'ActivationDate' + payload['ActivationDate']
    url_id += 'DeactivationDate' + payload['DeactivationDate']

    try:
        with open(url_id + '.p', 'rb') as f:
            response = pickle.load(f)
        print('CACHED')
    except FileNotFoundError:
        response = requests.get(
            full_url,
            params=payload,
            auth=(SD_USER, SD_PASSWORD)
        )
        with open(url_id + '.p', 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    xml_response = xmltodict.parse(response.text)[url]
    return xml_response


if __name__ == '__main__':
    _sd_lookup(SD_CHANGES[0])
