import os
import json
import pathlib
import logging

logger = logging.getLogger("AdReader")


# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
# TODO: This must be clean up, settings should be loaded by __init__
# and no references should be needed in global scope.
SETTINGS = json.loads(cfg_file.read_text())


def _read_global_settings():
    global_settings = {}

    ad_servers_raw = os.environ.get('AD_SERVERS')
    if ad_servers_raw:
        global_settings['servers'] = set(ad_servers_raw.split(' '))
    else:
        global_settings['servers'] = None

    global_settings['winrm_host'] = SETTINGS.get('integrations.ad.winrm_host')
    if not global_settings['winrm_host']:
        msg = 'Missing hostname for remote management server'
        logger.error(msg)
        raise Exception(msg)
    return global_settings


def _read_primary_ad_settings():
    primary_settings = {}
    primary_settings['search_base'] = SETTINGS.get('integrations.ad.search_base')
    primary_settings['cpr_field'] = SETTINGS.get('integrations.ad.cpr_field')
    primary_settings['system_user'] = SETTINGS.get('integrations.ad.system_user')
    primary_settings['password'] = SETTINGS.get('integrations.ad.password')
    primary_settings['properties'] = SETTINGS.get('integrations.ad.properties')

    missing = []
    for key, val in primary_settings.items():
        if not val:
            missing.append(key)
    if missing:
        msg = 'Missing values for {}'.format(missing)
        logger.error(msg)
        raise Exception(msg)

    # Settings that do not need to be set, or have defaults
    primary_settings['server'] = os.environ.get('AD_SERVER')

    # So far false in all known cases, default to false
    get_ad_object = os.environ.get('AD_GET_AD_OBJECT', 'False')
    primary_settings['get_ad_object'] = get_ad_object.lower() == 'true'
    return primary_settings


def _read_primary_write_information():
    """
    Read the configuration for writing to the primary AD. If anything is missing,
    the AD write will be disabled.
    """
    primary_write_settings = {}

    # Shared with read
    primary_write_settings['cpr_field'] = os.environ.get('AD_CPR_FIELD')

    # Field for writing the uuid of a user, used to sync to STS
    primary_write_settings['uuid_field'] = os.environ.get('AD_WRITE_UUID')

    # Field for writing the name of the users 'forvaltning'
    primary_write_settings['forvaltning_field'] = os.environ.get('AD_WRITE_FORVALTNING')

    # Field for the path to the users unit
    primary_write_settings['org_field'] = os.environ.get('AD_WRITE_ORG')
    missing = []
    for key, val in primary_write_settings.items():
        if not val:
            missing.append(key)
    if missing:
        msg = 'Missing values for AD write {}'.format(missing)
        logger.error(msg)
        primary_write_settings = {}

    return primary_write_settings


def _read_school_ad_settings():
    school_settings = {}

    school_settings['search_base'] = os.environ.get('AD_SCHOOL_SEARCH_BASE')
    school_settings['cpr_field'] = os.environ.get('AD_SCHOOL_CPR_FIELD')
    school_settings['system_user'] = os.environ.get('AD_SCHOOL_SYSTEM_USER')
    school_settings['password'] = os.environ.get('AD_SCHOOL_PASSWORD')
    ad_school_prop_raw = os.environ.get('AD_SCHOOL_PROPERTIES')
    if ad_school_prop_raw:
        school_settings['properties'] = set(ad_school_prop_raw.split(' '))
    else:
        school_settings['properties'] = None

    missing = []
    for key, val in school_settings.items():
        if not val:
            missing.append(key)
    if missing:
        msg = 'Missing values for {}, skiping school AD'.format(missing)
        logger.info(msg)
        school_settings['read_school'] = False
    else:
        school_settings['read_school'] = True

    # Settings that do not need to be set
    school_settings['server'] = os.environ.get('AD_SCHOOL_SERVER')

    # So far true in all known cases, default to true
    get_ad_object = os.environ.get('AD_SCHOOL_GET_AD_OBJECT', 'True')
    school_settings['get_ad_object'] = get_ad_object.lower() == 'true'

    return school_settings


def read_settings_from_env():
    settings = {}
    settings['global'] = _read_global_settings()
    settings['primary'] = _read_primary_ad_settings()
    settings['school'] = _read_school_ad_settings()
    settings['primary_write'] = _read_primary_write_information()
    return settings
