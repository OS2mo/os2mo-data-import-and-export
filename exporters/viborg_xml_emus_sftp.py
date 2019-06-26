import io
import os
import datetime
import logging
from spsftp import SpSftp
from os2mo_helpers.mora_helpers import MoraHelper
from viborg_xml_emus import main as generate_file

logger = logging.getLogger("emus-sftp")

logger.info("checking environment")
MORA_BASE = os.environ.get('MORA_BASE', 'localhost:80')
try:
    SFTP_USER = os.environ["SFTP_USER"]
    SFTP_HOST = os.environ["SFTP_HOST"]
    SFTP_KEY_PATH = os.environ["SFTP_KEY_PATH"]
    SFTP_KEY_PASSPHRASE = os.environ["SFTP_KEY_PASSPHRASE"]
    MUSSKEMA_RECIPIENT = os.environ["MUSSKEMA_RECIPIENT"]
except Exception as e:
    logger.error(e)
    raise EnvironmentError(str(e))


def main():
    logger.info("generating file for transfer")
    generated_file = io.StringIO()
    generate_file(
        emus_xml_file=generated_file,
        mh=MoraHelper(MORA_BASE)
    )
    logger.info("encoding file for transfer")
    filetosend = io.BytesIO(generated_file.getvalue().encode("utf-8"))

    sp = SpSftp({
        "user": SFTP_USER,
        "host": SFTP_HOST,
        "ssh_key_path": SFTP_KEY_PATH,
        "ssh_key_passphrase": SFTP_KEY_PASSPHRASE
    })

    sp.connect()
    filename = datetime.datetime.now().strftime(
        "%Y%m%d_%H%M%S_os2mo2musskema.xml"
    )
    logger.info("sending %s to %s", filename, MUSSKEMA_RECIPIENT)
    sp.send(filetosend, filename, MUSSKEMA_RECIPIENT)
    sp.disconnect()


if __name__ == '__main__':
    main()
