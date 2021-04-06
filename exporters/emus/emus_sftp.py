import datetime
import logging
import os

import click
from spsftp import SpSftp

from exporters.utils.load_settings import load_settings

logger = logging.getLogger("emus-sftp")


@click.command()
@click.argument('filename')
def main(filename):
    logger.info("reading top_settings")
    top_settings = load_settings()
    SFTP_USER = top_settings["emus.sftp_user"]
    SFTP_HOST = top_settings["emus.sftp_host"]
    SFTP_KEY_PATH = top_settings["emus.sftp_key_path"]
    SFTP_KEY_PASSPHRASE = top_settings["emus.sftp_key_passphrase"]
    MUSSKEMA_RECIPIENT = top_settings["emus.recipient"]
    QUERY_EXPORT_DIR = top_settings["mora.folder.query_export"]
    EMUS_FILENAME = top_settings.get("emus.outfile_name", "emus_filename.xml")

    logger.info("encoding file for transfer")
    with open(filename, "r", encoding="utf-8") as generated_file:
        filetosend = generated_file.read()

    logger.info("connecting sftp")
    try:
        sp = SpSftp(
            {
                "user": SFTP_USER,
                "host": SFTP_HOST,
                "ssh_key_path": SFTP_KEY_PATH,
                "ssh_key_passphrase": SFTP_KEY_PASSPHRASE,
            }
        )
    except Exception:
        logger.exception("error in sftp connection")
        raise
    sp.connect()

    filename = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_os2mo2musskema.xml")
    logger.info("sending %s to %s", filename, MUSSKEMA_RECIPIENT)
    sp.send(filetosend, filename, MUSSKEMA_RECIPIENT)
    sp.disconnect()

    # write the file that is sent into query export dir too
    if QUERY_EXPORT_DIR and EMUS_FILENAME:
        filepath = os.path.join(QUERY_EXPORT_DIR, os.path.basename(EMUS_FILENAME))
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(generated_file.getvalue())

    logger.info("program ended")


if __name__ == "__main__":
    main()
