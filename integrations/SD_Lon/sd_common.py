import os
import pickle
import logging
import hashlib
import requests
import xmltodict
from pathlib import Path
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
    cache_file = Path('sd_' + lookup_id + '.p')

    if cache_file.is_file():
        with open(str(cache_file), 'rb') as f:
            response = pickle.load(f)
        logger.info('This SD lookup was found in cache: {}'.format(lookup_id))
    else:
        response = requests.get(
            full_url,
            params=payload,
            auth=(SD_USER, SD_PASSWORD)
        )
        with open(str(cache_file), 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    dict_response = xmltodict.parse(response.text)
    if url in dict_response:
        xml_response = dict_response[url]
    else:
        logger.error('Envelope: {}'.format(dict_response['Envelope']))
        xml_response = {}
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


def engagement_types(helper):
    """
    Read the engagement types from MO and match them up against the four
    known types in the SD->MO import.
    :param helper: An instance of mora-helpers.
    :return: A dict matching up the engagement types with LoRa class uuids.
    """
    # These constants are global in all SD municipalities (because they are created
    # by the SD->MO importer.
    PRIMARY = 'Ansat'
    NO_SALARY = 'status0'
    NON_PRIMARY = 'non-primary'
    FIXED_PRIMARY = 'explicitly-primary'

    logger.info('Read engagement types')
    primary = None
    no_salary = None
    non_primary = None
    fixed_primary = None

    engagement_types = helper.read_classes_in_facet('engagement_type')
    for engagement_type in engagement_types[0]:
        if engagement_type['user_key'] == PRIMARY:
            primary = engagement_type['uuid']
        if engagement_type['user_key'] == NON_PRIMARY:
            non_primary = engagement_type['uuid']
        if engagement_type['user_key'] == NO_SALARY:
            no_salary = engagement_type['uuid']
        if engagement_type['user_key'] == FIXED_PRIMARY:
            fixed_primary = engagement_type['uuid']

    type_uuids = {
        'primary': primary,
        'non_primary': non_primary,
        'no_salary': no_salary,
        'fixed_primary': fixed_primary
    }
    if None in type_uuids.values():
        raise Exception('Missing engagements types: {}'.format(type_uuids))
    return type_uuids
