from socket import timeout  # Used to handle exception

import click

from ftplib import FTP_TLS

from ra_utils.load_settings import load_settings


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


SETTINGS = load_settings()


@click.command()
@click.argument("from_file")
@click.argument("to_file")
def main(from_file, to_file):
    print('Directory listing before upload:')
    dir_list()

    # The server insists on returning a timeout after every upload, so we
    # re-start the connection for each file.
    ftps = start_ftpes_connection()

    with open(from_file, 'rb') as csv_file:
        print('Uploading: {}'.format(to_file))
        try:
            ftps.storbinary('STOR {}'.format(to_file), csv_file)
        except ConnectionResetError:
            print("Connection Error")
            pass
        except timeout:
            print("Timeout Error")
            pass
        print('Done')
        ftps.quit()

    print('Directory listing after upload:')
    dir_list()


if __name__ == '__main__':
    main()
