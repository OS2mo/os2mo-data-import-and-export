import json
import pathlib
import argparse
import requests_kerberos
from winrm import Session


# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
# TODO: This must be clean up, settings should be loaded by __init__
# and no references should be needed in global scope.
SETTINGS = json.loads(cfg_file.read_text())
WINRM_HOST = SETTINGS.get('integrations.ad.winrm_host')
if not (WINRM_HOST):
    raise Exception('WINRM_HOST is missing')


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
    for user in ad_reader.results.values():
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
    print('Test that AD write settings are set up')
    write_settings = test_ad_write_settings()
    if not write_settings:
        print('Write settings not correctly set up')
        exit(1)

    # TODO: If we could make a test write, it would be nice.

    print('Success')
    exit(0)


def cli():
    """
    Command line interface for the AD writer class.
    """
    parser = argparse.ArgumentParser(description='AD Writer')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--test-read-settings', action='store_true')
    group.add_argument('--test-write-settings', action='store_true')

    args = vars(parser.parse_args())

    if args.get('test_read_settings'):
        perform_read_test()

    if args.get('test_write_settings'):
        perform_write_test()


if __name__ == '__main__':
    # perform_read_test()
    cli()
