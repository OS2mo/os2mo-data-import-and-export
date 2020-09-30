import io
import os
import datetime
import json
import logging
import pathlib
from spsftp import SpSftp
from exporters.emus.lcdb_viborg_xml_emus import config, main as generate_file


logger = logging.getLogger("emus-sftp")

logger.info("reading top_settings")

cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
top_settings = json.loads(cfg_file.read_text())

MORA_BASE = top_settings["mora.base"]
SFTP_USER = top_settings["emus.sftp_user"]
SFTP_HOST = top_settings["emus.sftp_host"]
SFTP_KEY_PATH = top_settings["emus.sftp_key_path"]
SFTP_KEY_PASSPHRASE = top_settings["emus.sftp_key_passphrase"]
MUSSKEMA_RECIPIENT = top_settings["emus.recipient"]
QUERY_EXPORT_DIR = top_settings["mora.folder.query_export"]
EMUS_FILENAME = top_settings.get("emus.outfile_name", 'emus_filename.xml')


def main():
    logger.info("generating file for transfer")
    generated_file = io.StringIO()
    generate_file(
        emus_xml_file=generated_file,
        settings=config.settings
    )
    logger.info("encoding file for transfer")
    filetosend = io.BytesIO(generated_file.getvalue().encode("utf-8"))

    logger.info("connecting sftp")
    try:
        sp = SpSftp({
            "user": SFTP_USER,
            "host": SFTP_HOST,
            "ssh_key_path": SFTP_KEY_PATH,
            "ssh_key_passphrase": SFTP_KEY_PASSPHRASE
        })
    except Exception:
        logger.exception("error in sftp connection")
        raise

    sp.connect()
    filename = datetime.datetime.now().strftime(
        "%Y%m%d_%H%M%S_os2mo2musskema.xml"
    )
    logger.info("sending %s to %s", filename, MUSSKEMA_RECIPIENT)
    sp.send(filetosend, filename, MUSSKEMA_RECIPIENT)
    sp.disconnect()

    # write the file that is sent into query export dir too
    if QUERY_EXPORT_DIR and EMUS_FILENAME:
        filepath = os.path.join(QUERY_EXPORT_DIR, os.path.basename(EMUS_FILENAME))
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(generated_file.getvalue())

    logger.info("program ended")


if __name__ == '__main__':
    main()
