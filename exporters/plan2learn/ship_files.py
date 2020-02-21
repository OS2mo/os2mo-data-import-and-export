import json
import socket  # Used to handle exception

from pathlib import Path
from ftplib import FTP_TLS


def start_ftpes_connection(timeout=30):
    ftps = FTP_TLS(timeout=timeout)
    ftps.connect(SETTINGS['exporters.plan2learn.host'], 21)
    ftps.auth()
    ftps.prot_p()
    ftps.login(SETTINGS['exporters.plan2learn.user'],
               SETTINGS['exporters.plan2learn.password'])
    return ftps


def dir_list():
    ftps = start_ftpes_connection()
    dir_list = []
    ftps.dir(dir_list.append)
    for file_info in dir_list:
        print(file_info)
    ftps.quit()


cfg_file = Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())


filenames = [
    'organisation.csv',
    'bruger.csv',
    'engagement.csv',
    'leder.csv',
    'stillingskode.csv'
]

print('Directory listing before upload:')
dir_list()

cwd = Path(__file__).resolve().parent
for filename in filenames:
    filepath = Path(__file__).resolve().parent / filename

    # The server insists on returning a timeout after every upload, so we
    # re-start the connection for each file.
    ftps = start_ftpes_connection()

    with open(str(filepath), 'rb') as csv_file:
        print('Uploading: {}'.format(filename))
        try:
            ftps.storbinary('STOR {}'.format(filename), csv_file)
        except ConnectionResetError:
            pass
        except socket.timeout:
            pass
        print('Done')
        ftps.quit()
