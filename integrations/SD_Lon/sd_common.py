import uuid
import json
import pickle
import pathlib
import logging
import hashlib
import requests
import xmltodict
from pathlib import Path
logger = logging.getLogger("sdCommon")

# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

INSTITUTION_IDENTIFIER = SETTINGS['integrations.SD_Lon.institution_identifier']
SD_USER = SETTINGS['integrations.SD_Lon.sd_user']
SD_PASSWORD = SETTINGS['integrations.SD_Lon.sd_password']

if not (INSTITUTION_IDENTIFIER and SD_USER and SD_PASSWORD):
    raise Exception('Credentials missing')


def sd_lookup(url, params={}, use_cache=True):
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

    cache_dir = Path('tmp/')
    if not cache_dir.is_dir():
        raise Exception('Folder for temporary files does not exist')

    cache_file = Path('tmp/sd_' + lookup_id + '.p')

    if cache_file.is_file() and use_cache:
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


def mora_assert(response):
    """ Check response is as expected """
    assert response.status_code in (200, 201, 400, 404)
    if response.status_code == 400:
        # Check actual response
        assert response.text.find('not give raise to a new registration') > 0
        logger.debug('Requst had no effect')
    return None


def generate_uuid(value, org_id_prefix, org_name=None):
    """
    Code almost identical to this also lives in the Opus importer.
    """
    if org_id_prefix:
        base_hash = hashlib.md5(org_id_prefix.encode())
    else:
        base_hash = hashlib.md5(org_name.encode())

    base_digest = base_hash.hexdigest()
    base_uuid = uuid.UUID(base_digest)

    combined_value = (str(base_uuid) + str(value)).encode()
    value_hash = hashlib.md5(combined_value)
    value_digest = value_hash.hexdigest()
    value_uuid = str(uuid.UUID(value_digest))
    return value_uuid


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
