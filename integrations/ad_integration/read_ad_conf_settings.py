import os
import logging

logger = logging.getLogger("AdReader")


def _read_global_settings():
    global_settings = {}
    global_settings['winrm_host'] = os.environ.get('WINRM_HOST')

    if not global_settings['winrm_host']:
        msg = 'Missing hostname for remote management server'
        logger.error(msg)
        raise Exception(msg)
    return global_settings


def _read_primary_ad_settings():
    primary_settings = {}

    primary_settings['search_base'] = os.environ.get('AD_SEARCH_BASE')
    primary_settings['cpr_field'] = os.environ.get('AD_CPR_FIELD')
    primary_settings['system_user'] = os.environ.get('AD_SYSTEM_USER')
    primary_settings['password'] = os.environ.get('AD_PASSWORD')
    ad_properties_raw = os.environ.get('AD_PROPERTIES')
    if ad_properties_raw:
        primary_settings['properties'] = set(ad_properties_raw.split(' '))
    else:
        primary_settings['properties'] = None

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
    return settings
