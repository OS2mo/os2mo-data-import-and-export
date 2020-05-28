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

    global_settings['servers'] = SETTINGS.get('integrations.ad')[0]['servers']
    global_settings['winrm_host'] = SETTINGS.get('integrations.ad.winrm_host')
    if not global_settings['winrm_host']:
        msg = 'Missing hostname for remote management server'
        logger.error(msg)
        raise Exception(msg)
    return global_settings


    #global_settings = {"instances":SETTINGS.get('integrations.ad')}
def _read_primary_ad_settings(index=0):
    primary_settings = {}
    primary_settings['servers'] = SETTINGS.get('integrations.ad')[index]['servers']
    primary_settings['search_base'] = SETTINGS.get('integrations.ad')[index]["search_base"]
    primary_settings['cpr_field'] = SETTINGS.get('integrations.ad')[index]["cpr_field"]
    primary_settings['system_user'] = SETTINGS.get('integrations.ad')[index]["system_user"]
    primary_settings['password'] = SETTINGS.get('integrations.ad')[index]['password']
    primary_settings['properties'] = SETTINGS.get('integrations.ad')[index]["properties"]

    missing = []
    for key, val in primary_settings.items():
        if val is None:
            missing.append(key)
    if missing:
        msg = 'Missing values for {}'.format(missing)
        logger.error(msg)
        raise Exception(msg)

    # 36182 exclude non primary AD-users
    primary_settings["discriminator.field"] = SETTINGS.get("integrations.ad")[index].get("discriminator.field")
    if primary_settings["discriminator.field"] is not None:
        # if we have a field we MUST have .values and .function
        primary_settings["discriminator.values"] = SETTINGS["integrations.ad"][index]["discriminator.values"]
        primary_settings["discriminator.function"] = SETTINGS["integrations.ad"][index]["discriminator.function"]
        if not primary_settings["discriminator.function"] in ["include", "exclude"]:
            raise ValueError("'ad.discriminator.function' must be include or exclude for AD %d"% index)

    # Settings that do not need to be set, or have defaults
    primary_settings['server'] = None
    primary_settings['sam_filter'] = SETTINGS.get("integrations.ad")[index].get("sam_filter", '')
    primary_settings['cpr_separator'] = SETTINGS.get('integrations.ad')[index].get('cpr_separator', '')
    primary_settings['ad_mo_sync_mapping'] = SETTINGS.get('integrations.ad')[index].get('ad_mo_sync_mapping', {})

    # So far false in all known cases, default to false
    # get_ad_object = os.environ.get('AD_GET_AD_OBJECT', 'False')
    # primary_settings['get_ad_object'] = get_ad_object.lower() == 'true'
    primary_settings['get_ad_object'] = False
    return primary_settings


def _read_primary_write_information():
    """
    Read the configuration for writing to the primary AD. If anything is missing,
    the AD write will be disabled.
    """
    # TODO: Some happy day, we could check for the actual validity of these
    primary_write_settings = {}

    # Shared with read
    primary_write_settings['cpr_field'] = SETTINGS.get('integrations.ad')[0]['cpr_field']

    # Field for writing the uuid of a user, used to sync to STS
    primary_write_settings['uuid_field'] = SETTINGS.get(
        'integrations.ad.write.uuid_field')

    # Field for writing the name of the users level2orgunit (eg direktørområde)
    primary_write_settings['level2orgunit_field'] = SETTINGS.get(
        'integrations.ad.write.level2orgunit_field')

    # Field for the path to the users unit
    primary_write_settings['org_field'] = SETTINGS.get(
        'integrations.ad.write.org_unit_field')

    # Word to go after @ in UPN
    primary_write_settings['upn_end'] = SETTINGS.get(
        'integrations.ad.write.upn_end')

    # These are technically speaking not used in this context, but it is needed for
    # AD write and can benifit from the automated check.

    # UUID for the unit type considered to be level2orgunit
    primary_write_settings['level2orgunit_type'] = SETTINGS.get(
        'integrations.ad.write.level2orgunit_type')

    missing = []

    for key, val in primary_write_settings.items():
        if val is None:
            missing.append(key)
    if len(missing) > 0:
        msg = 'Missing values for AD write {}'.format(missing)
        logger.info(msg)
        return {}

    # Check for illegal configuration of AD Write.
    mo_to_ad_fields = SETTINGS.get('integrations.ad_writer.mo_to_ad_fields', {})
    ad_field_names = list(mo_to_ad_fields.values()) + [
        primary_write_settings['org_field'],
        primary_write_settings['level2orgunit_field'],
        primary_write_settings['uuid_field']
    ]
    print(ad_field_names)
    if len(ad_field_names) > len(set(ad_field_names)):
        msg = 'Duplicate AD fieldnames in settings: {}'
        logger.info(msg.format(list(sorted(ad_field_names))))
        primary_write_settings = {}

    return primary_write_settings


def _NEVER_read_school_ad_settings():
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
    school_settings['cpr_separator'] = SETTINGS.get(
        'integrations.ad.school_cpr_separator', '')

    # So far true in all known cases, default to true
    get_ad_object = os.environ.get('AD_SCHOOL_GET_AD_OBJECT', 'True')
    school_settings['get_ad_object'] = get_ad_object.lower() == 'true'

    return school_settings


def read_settings(index=0):
    settings = {}
    settings['global'] = _read_global_settings()
    settings['primary'] = _read_primary_ad_settings(index)
    settings['primary_write'] = _read_primary_write_information()
    return settings


if __name__ == '__main__':
    read_settings()
