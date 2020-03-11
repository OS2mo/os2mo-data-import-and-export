import json
import pathlib
import logging
import argparse
import requests
import requests_kerberos

from winrm import Session
from integrations.ad_integration import ad_logger


cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
# TODO: This must be clean up, settings should be loaded by __init__
# and no references should be needed in global scope.
SETTINGS = json.loads(cfg_file.read_text())
WINRM_HOST = SETTINGS.get('integrations.ad.winrm_host')
if not (WINRM_HOST):
    raise Exception('WINRM_HOST is missing')

logger = logging.getLogger('AdTestConnectivity')


def test_basic_connectivity():
    session = Session(
        'http://{}:5985/wsman'.format(WINRM_HOST),
        transport='kerberos',
        auth=(None, None)
    )
    try:
        r = session.run_cmd('ipconfig', ['/all'])
        error = None
    except requests_kerberos.exceptions.KerberosExchangeError as e:
        error = str(e)
    except requests.exceptions.ConnectionError as e:
        error = 'Unable to contact winrm_host {}, message: {}'
        error = error.format(WINRM_HOST, e)
    if error is None and r.status_code == 0:
        return True
    else:
        print('Error')
        if error is None:
            print(r.std_err)
        else:
            print(error)
        return False


def test_ad_contact():
    from ad_reader import ADParameterReader
    ad_reader = ADParameterReader()
    response = ad_reader.read_encoding()
    if 'WindowsCodePage' in response:
        return True
    else:
        print('Error:')
        print(response)
        return False


def test_full_ad_read():
    """
    Test that we can read actual users from AD. This ensures suitable rights
    to access cpr-information in AD.
    """
    from ad_reader import ADParameterReader
    ad_reader = ADParameterReader()

    ad_reader.uncached_read_user(cpr='3111*')
    if ad_reader.results:
        print('Found users with bithday 31. November!')
        return False

    ad_reader.uncached_read_user(cpr='30*')
    if not ad_reader.results:
        print('No users found with bithday on the 30th of any month!')
        return False

    test_chars = {
        'æ': False,
        'ø': False,
        'å': False,
        '@': False
    }
    # Run through a set of users and test encoding and cpr-separator.
    for user in ad_reader.results.values():
        cpr = user[SETTINGS['integrations.ad.cpr_field']]
        separator = SETTINGS['integrations.ad.cpr_separator']

        cpr_ok = False
        if separator == '' and len(cpr) == 10:
            cpr_ok = True
        if cpr.count(separator) == 1 and len(cpr) == 11:
            cpr_ok = True
        if not cpr.replace(separator, '').isdigit():
            cpr_ok = False
        if not cpr_ok:
            msg = 'CPR-is not valid according to settings: {}, cpr-length: {}'
            print(msg.format(user['Name'], len(cpr)))
            return False

        for value in user.values():
            for char in test_chars.keys():
                if str(value).find(char) > -1:
                    test_chars[char] = True

    for test_value in test_chars.values():
        if not test_value:
            print('Did find any occurances of special char: {}'.format(test_chars))
            print('(all should be True for success)')
            return False

    return True


def test_ad_write_settings():
    from integrations.ad_integration import read_ad_conf_settings
    all_settings = read_ad_conf_settings.read_settings()
    if not all_settings['primary_write']:
        return False
    else:
        return True


def perform_read_test():
    print('Test basic connectivity (Kerberos)')
    basic_connection = test_basic_connectivity()
    if not basic_connection:
        print('Unable to connect to management server')
        exit(1)

    print('Test AD contact')
    ad_connection = test_ad_contact()
    if not ad_connection:
        print('Unable to connect to AD')
        exit(1)

    print('Test ability to read from AD')
    full_ad_read = test_full_ad_read()
    if not full_ad_read:
        print('Unable to read users from AD correctly')
        exit(1)

    print('Success')
    exit(0)


def perform_write_test():
    from ad_reader import ADParameterReader
    ad_reader = ADParameterReader()

    print('Test that AD write settings are set up')
    write_settings = test_ad_write_settings()
    if not write_settings:
        print('Write settings not correctly set up')
        exit(1)

    # TODO: If we could make a test write, it would be nice.

    print('Write settings set up correctly')

    user_info_list = []
    i = 0
    logger.info('Check for availability of needed fields - find some test users')
    print('Check for availability of needed fields - find some test users')

    minumum_expected_fields = {
        SETTINGS['integrations.ad.write.uuid_field']: False,
        SETTINGS['integrations.ad.write.forvaltning_field']: False,
        SETTINGS['integrations.ad.write.org_unit_field']: False
    }

    while len(user_info_list) < 10:
        i = i + 1
        if i > 31:
            msg = 'Cannot find 10 users - unable to test for avaiable fields'
            print(msg)
            logger.error(msg)
            exit(1)
        msg = 'So far found {} users, current cpr: {}'
        logger.info(msg.format(len(user_info_list), str(i).zfill(2)))
        user_info = ad_reader.get_from_ad(cpr=str(i).zfill(2) + '*')
        user_info_list = user_info_list + user_info

    msg = 'Found more than 10 users ({}) - now test for avaiable fields'
    print(msg.format(len(user_info_list)))
    logger.info(msg.format(len(user_info_list)))

    for user in user_info_list:
        if isinstance(user, str):
            print('Unable to read from AD: {}'.format(user))
            exit(1)

        for prop in user['PropertyNames']:
            if prop in minumum_expected_fields:
                minumum_expected_fields[prop] = True

    if False in minumum_expected_fields.values():
        print('An import field is now found on the tested users')
        print(minumum_expected_fields)
        exit(1)
    else:
        print('Test of AD fields for writing is a success')
        exit(0)


def cli():
    """
    Command line interface for the AD writer class.
    """
    parser = argparse.ArgumentParser(description='AD Writer')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--test-read-settings', action='store_true')
    group.add_argument('--test-write-settings', action='store_true')

    args = vars(parser.parse_args())

    if args.get('test_read_settings'):
        perform_read_test()

    if args.get('test_write_settings'):
        perform_write_test()


if __name__ == '__main__':
    ad_logger.start_logging('test_connectivity.log')
    cli()
