import json
import pathlib
# from kerberos import GSSError
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


def perform_test():
    basic_connection = test_basic_connectivity()
    if not basic_connection:
        print('Unable to connect to management server')
        exit()

    ad_connection = test_ad_contact()
    if not ad_connection:
        print('Unable to connect to AD')
        exit()

    print('Success')


if __name__ == '__main__':
    perform_test()
