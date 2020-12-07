import uuid
import json
import pickle
import pathlib
import logging
import hashlib
import requests
import xmltodict
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
logger = logging.getLogger("sdCommon")


@lru_cache(maxsize=None)
def load_settings():
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No settings file: ' + str(cfg_file))
    # TODO: This must be clean up, settings should be loaded by __init__
    # and no references should be needed in global scope.
    return json.loads(cfg_file.read_text())


@lru_cache(maxsize=None)
def sd_lookup_settings():
    settings = load_settings()

    institution_identifier = settings['integrations.SD_Lon.institution_identifier']
    if not institution_identifier:
        raise ValueError("Missing setting, institution_identifier")

    sd_user = settings['integrations.SD_Lon.sd_user']
    if not sd_user:
        raise ValueError("Missing setting, sd_user")

    sd_password = settings['integrations.SD_Lon.sd_password']
    if not sd_password:
        raise ValueError("Missing setting, sd_password")

    return institution_identifier, sd_user, sd_password


def _sd_lookup_cache(func):
    # We need a cache dir to exist before we can proceed
    cache_dir = Path('tmp/')
    if not cache_dir.is_dir():
        raise Exception('Folder for temporary files does not exist')

    def create_hex_digest(full_url, payload):
        """Create a reproducible hex digest from url and payloads."""
        hasher = hashlib.sha256()

        for key, value in sorted(payload.items()):
            hasher.update((str(key) + str(value)).encode())
        hasher.update(full_url.encode())

        return hasher.hexdigest()

    def write_response(cache_file, response):
        """Write response to disk."""
        with open(str(cache_file), 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    def read_response(cache_file):
        """Read response from disk."""
        with open(str(cache_file), 'rb') as f:
            response = pickle.load(f)
        return response

    @wraps(func)
    def wrapper(full_url, payload, auth, use_cache=True):
        # Short-circuit as noop, if no caching is requested
        if use_cache == False:
            return func(full_url, payload, auth)

        # Create digest and find filename
        lookup_id = create_hex_digest(full_url, payload)
        cache_file = Path('tmp/sd_' + lookup_id + '.p')

        # If cache file was found, use it
        if cache_file.is_file():
            response = read_response(cache_file)
            logger.info('This SD lookup was found in cache: {}'.format(lookup_id))
            print(full_url, "read from cache")
        else:  # No cache
            response = func(full_url, payload, auth)
            write_response(cache_file, response)
            print(full_url, "requested from SD")
        return response
    return wrapper


@_sd_lookup_cache
def _sd_request(full_url, payload, auth):
    """Fire the actual request against SD.

    Annotation only calls this if we did not hit the cache.
    """
    return requests.get(
        full_url,
        params=payload,
        auth=auth,
    )


def sd_lookup(url, params={}, use_cache=True):
    """Fire a requests against SD.

    Utilizes _sd_request to fire the actual request, which in turn utilize
    _sd_lookup_cache for caching.
    """
    logger.info('Retrieve: {}'.format(url))
    logger.debug('Params: {}'.format(params))

    BASE_URL = 'https://service.sd.dk/sdws/'
    full_url = BASE_URL + url

    institution_identifier, sd_user, sd_password = sd_lookup_settings()

    payload = {
        'InstitutionIdentifier': institution_identifier,
    }
    payload.update(params)
    auth=(sd_user, sd_password)
    response = _sd_request(full_url, payload, auth, use_cache=use_cache)

    dict_response = xmltodict.parse(response.text)

    if url in dict_response:
        xml_response = dict_response[url]
    else:
        msg = 'SD api error, envelope: {}'
        logger.error(msg.format(dict_response['Envelope']))
        raise Exception(msg.format(dict_response['Envelope']))
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
    """Check response is as expected."""
    assert response.status_code in (200, 201, 400, 404), response.status_code
    if response.status_code == 400:
        # Check actual response
        assert response.text.find('not give raise to a new registration') > 0, response.text
        logger.debug('Request had no effect')
    return None


def generate_uuid(value, org_id_prefix, org_name=None):
    """
    Code almost identical to this also lives in the Opus importer.
    """
    # TODO: Refactor to avoid duplication
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


def primary_types(helper):
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

    logger.info('Read primary types')
    primary = None
    no_salary = None
    non_primary = None
    fixed_primary = None

    primary_types = helper.read_classes_in_facet('primary_type')
    for primary_type in primary_types[0]:
        if primary_type['user_key'] == PRIMARY:
            primary = primary_type['uuid']
        if primary_type['user_key'] == NON_PRIMARY:
            non_primary = primary_type['uuid']
        if primary_type['user_key'] == NO_SALARY:
            no_salary = primary_type['uuid']
        if primary_type['user_key'] == FIXED_PRIMARY:
            fixed_primary = primary_type['uuid']

    type_uuids = {
        'primary': primary,
        'non_primary': non_primary,
        'no_salary': no_salary,
        'fixed_primary': fixed_primary
    }
    if None in type_uuids.values():
        raise Exception('Missing primary types: {}'.format(type_uuids))
    return type_uuids


class EmploymentStatus(Enum):
    """Corresponds to EmploymentStatusCode from SD.

    Employees usually start in AnsatUdenLoen, and then change to AnsatMedLoen.
    This will usually happen once they actually have their first day at work.

    From AnsatMedLoen they can somewhat freely transfer to the other statusses.
    This includes transfering back to AnsatMedLoen from any other status.

    Note for instance, that it is entirely possible to be Ophoert and then get
    hired back, and thus go from Ophoert to AnsatMedLoen.

    There is only one terminal state, namely Slettet, wherefrom noone will
    return. This state is invoked from status 7-8-9 after a few years.

    Status Doed will probably only migrate to status slettet, but there are no
    guarantees given.
    """
    # This status most likely represent not yet being at work
    AnsatUdenLoen = '0'

    # These statusses represent being at work
    AnsatMedLoen = '1'
    Overlov = '3'

    # These statusses represent being let go
    Migreret = '7'
    Ophoert = '8'
    Doed = '9'

    # This status is the special terminal state
    Slettet = 'S'


Employeed = [
    EmploymentStatus.AnsatUdenLoen,
    EmploymentStatus.AnsatMedLoen,
    EmploymentStatus.Overlov
]
LetGo = [
    EmploymentStatus.Migreret,
    EmploymentStatus.Ophoert,
    EmploymentStatus.Doed
]
OnPayroll = [
    EmploymentStatus.AnsatMedLoen,
    EmploymentStatus.Overlov
]
