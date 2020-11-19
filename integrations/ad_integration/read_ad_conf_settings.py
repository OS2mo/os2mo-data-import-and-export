import os
import json
import pathlib
import logging

logger = logging.getLogger("AdReader")

from integrations.ad_integration.utils import LazyDict


def _read_global_settings(top_settings):
    global_settings = {}

    global_settings['servers'] = top_settings.get('integrations.ad.write.servers')
    global_settings['winrm_host'] = top_settings.get('integrations.ad.winrm_host')
    global_settings['mora.base'] = top_settings.get('mora.base')
    if not global_settings['winrm_host']:
        msg = 'Missing hostname for remote management server'
        logger.error(msg)
        raise Exception(msg)
    return global_settings


def _read_primary_ad_settings(top_settings):
    primary_settings = {}
    primary_settings['search_base'] = top_settings.get('integrations.ad.search_base')
    primary_settings['cpr_field'] = top_settings.get('integrations.ad.cpr_field')
    primary_settings['system_user'] = top_settings.get('integrations.ad.system_user')
    primary_settings['password'] = top_settings.get('integrations.ad.password')
    primary_settings['properties'] = top_settings.get('integrations.ad.properties')
    primary_settings['method'] = top_settings.get("integrations.ad.method", "kerberos")
    
    missing = []
    for key, val in primary_settings.items():
        if val is None:
            missing.append(key)
    if missing:
        msg = 'Missing values for {}'.format(missing)
        logger.error(msg)
        raise Exception(msg)

    # 36182 exclude non primary AD-users
    primary_settings['discriminator.field'] = top_settings.get('integrations.ad.discriminator.field')
    if primary_settings['discriminator.field'] is not None:
        # if we have a field we MUST have .values and .function
        primary_settings['discriminator.values'] = top_settings['integrations.ad.discriminator.values']
        primary_settings['discriminator.function'] = top_settings['integrations.ad.discriminator.function']
        if not primary_settings['discriminator.function'] in ["include", "exclude"]:
            raise ValueError("'ad.discriminator.function' must be include or exclude")

    # Settings that do not need to be set, or have defaults
    primary_settings['server'] = None
    primary_settings['sam_filter'] = top_settings.get('integrations.ad.sam_filter', '')
    primary_settings['caseless_samname'] = top_settings.get('integrations.ad.caseless_samname', '')
    primary_settings['cpr_separator'] = top_settings.get(
        'integrations.ad.cpr_separator', '')

    # So far false in all known cases, default to false
    # get_ad_object = os.environ.get('AD_GET_AD_OBJECT', 'False')
    # primary_settings['get_ad_object'] = get_ad_object.lower() == 'true'
    primary_settings['get_ad_object'] = False
    return primary_settings


def _read_primary_write_information(top_settings):
    """
    Read the configuration for writing to the primary AD. If anything is missing,
    the AD write will be disabled.
    """
    # TODO: Some happy day, we could check for the actual validity of these
    primary_write_settings = {}

    # Shared with read
    primary_write_settings['cpr_field'] = top_settings.get('integrations.ad.cpr_field')

    # Field for writing the uuid of a user, used to sync to STS
    primary_write_settings['uuid_field'] = top_settings.get(
        'integrations.ad.write.uuid_field')

    # Field for writing the name of the users level2orgunit (eg direktørområde)
    primary_write_settings['level2orgunit_field'] = top_settings.get(
        'integrations.ad.write.level2orgunit_field')

    # Field for the path to the users unit
    primary_write_settings['org_field'] = top_settings.get(
        'integrations.ad.write.org_unit_field')

    # Word to go after @ in UPN
    primary_write_settings['upn_end'] = top_settings.get(
        'integrations.ad.write.upn_end')

    # These are technically speaking not used in this context, but it is needed for
    # AD write and can benifit from the automated check.

    # Ordered list of primary engagements
    # Obsolete as of January 2020, will be removed.
    # primary_write_settings['primary_types'] = top_settings.get(
    # 'integrations.ad.write.primary_types')

    # UUID for the unit type considered to be level2orgunit
    primary_write_settings['level2orgunit_type'] = top_settings.get(
        'integrations.ad.write.level2orgunit_type'
    )

    missing = []

    for key, val in primary_write_settings.items():
        if not val:
            missing.append(key)
    if missing:
        msg = 'Missing values for AD write {}'.format(missing)
        logger.info(msg)
        return {}

    # Template fields
    primary_write_settings['mo_to_ad_fields'] = top_settings.get(
        'integrations.ad_writer.mo_to_ad_fields', {}
    )
    primary_write_settings['template_to_ad_fields'] = top_settings.get(
        'integrations.ad_writer.template_to_ad_fields', {}
    )

    # Check for illegal configuration of AD Write.
    mo_to_ad_fields = primary_write_settings['mo_to_ad_fields']
    template_to_ad_fields = primary_write_settings['template_to_ad_fields']
    ad_field_names = (
        list(mo_to_ad_fields.values()) +
        list(template_to_ad_fields.keys()) + [
            primary_write_settings['org_field'],
            primary_write_settings['level2orgunit_field'],
            primary_write_settings['uuid_field']
        ]
    )
    # Conflicts are case-insensitive
    ad_field_names = list(map(lambda ad_field: ad_field.lower(), ad_field_names))
    if len(ad_field_names) > len(set(ad_field_names)):
        msg = 'Duplicate AD fieldnames in settings: {}'
        logger.info(msg.format(sorted(ad_field_names)))
        primary_write_settings = {}

    return primary_write_settings


def _read_school_ad_settings(top_settings):
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
    school_settings['cpr_separator'] = top_settings.get(
        'integrations.ad.school_cpr_separator', '')

    # So far true in all known cases, default to true
    get_ad_object = os.environ.get('AD_SCHOOL_GET_AD_OBJECT', 'True')
    school_settings['get_ad_object'] = get_ad_object.lower() == 'true'

    return school_settings


def _load_settings_from_disk():
    # TODO: Soon we have done this 4 times. Should we make a small settings
    # importer, that will also handle datatype for specific keys?
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    # TODO: This must be clean up, settings should be loaded by __init__
    # and no references should be needed in global scope.
    settings = json.loads(cfg_file.read_text())
    return settings


SETTINGS = LazyDict()
SETTINGS.set_initializer(_load_settings_from_disk)


def read_settings(top_settings=SETTINGS):
    settings = {}
    settings['global'] = _read_global_settings(top_settings)
    settings['primary'] = _read_primary_ad_settings(top_settings)
    settings['school'] = _read_school_ad_settings(top_settings)
    settings['primary_write'] = _read_primary_write_information(top_settings)
    return settings


if __name__ == '__main__':
    read_settings()
