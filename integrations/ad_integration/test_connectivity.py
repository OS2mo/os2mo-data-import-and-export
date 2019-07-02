import os
from winrm import Session


WINRM_HOST = os.environ.get('WINRM_HOST', None)
if not (WINRM_HOST):
    raise Exception('WINRM_HOST is missing')


def test_basic_connectivity():
    session = Session(
        'http://{}:5985/wsman'.format(WINRM_HOST),
        transport='kerberos',
        auth=(None, None)
    )
    r = session.run_cmd('ipconfig', ['/all'])
    if r.status_code == 0:
        return True
    else:
        print('Error')
        print(r.std_err)
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
