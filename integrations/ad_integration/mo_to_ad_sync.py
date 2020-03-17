import os
import json
import logging
import pathlib

import ad_reader
import ad_writer
import ad_logger
import ad_exceptions

LOG_FILE = 'mo_to_ad_sync.log'
MO_UUID_FIELD = os.environ.get('AD_WRITE_UUID')
logger = logging.getLogger('MoAdSync')


def main():
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    settings = json.loads(cfg_file.read_text())

    mo_uuid_field = settings['integrations.ad.write.uuid_field']

    ad_logger.start_logging(LOG_FILE)
    reader = ad_reader.ADParameterReader()
    writer = ad_writer.ADWriter()

    all_users = reader.read_it_all()

    for user in all_users:
        if mo_uuid_field not in user:
            msg = 'User {} does not have a {} field - skipping'
            logger.info(msg.format(user['SamAccountName'], mo_uuid_field))
            continue
        msg = 'Now syncing: {}, {}'.format(user['SamAccountName'],
                                           user[mo_uuid_field])
        logger.info(msg)
        print(msg)
        try:
            response = writer.sync_user(user[mo_uuid_field], user)
            logger.debug('Respose to sync: {}'.format(response))
        except ad_exceptions.ManagerNotUniqueFromCprException:
            msg = 'Did not find a unique manager for {}'.format(user[mo_uuid_field])
            logger.error(msg)
        except ad_exceptions.UserNotFoundException:
            msg = 'User {}, {} with uuid {} was not found i MO, unable to sync'
            logger.error(msg.format(user['SamAccountName'], user['Name'],
                                    user[mo_uuid_field]))
        except Exception as e:
            logger.error('Unhandled exception: {}'.format(e))
            logger.exception("Unhandled exception:")


if __name__ == '__main__':
    main()
