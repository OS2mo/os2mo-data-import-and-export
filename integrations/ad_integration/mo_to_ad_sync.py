import os
import logging

import ad_reader
import ad_writer
import ad_logger
import ad_exceptions

LOG_FILE = 'mo_to_ad_sync.log'
MO_UUID_FIELD = os.environ.get('AD_WRITE_UUID')
logger = logging.getLogger('MoAdSync')


def main():
    ad_logger.start_logging(LOG_FILE)
    reader = ad_reader.ADParameterReader()
    writer = ad_writer.ADWriter()

    all_users = reader.read_it_all()
    for user in all_users:
        msg = 'Now syncing: {}, {}'.format(user['SamAccountName'],
                                           user[MO_UUID_FIELD])
        logger.info(msg)
        try:
            response = writer.sync_user(user[MO_UUID_FIELD], user)
            logger.debug('Respose to sync: {}'.format(response))
        except ad_exceptions.ManagerNotUniqueFromCprException:
            msg = 'Did ot find a unique manager for {}'.format(user[MO_UUID_FIELD])
            logger.error(msg)
        except Exception as e:
            logger.error('Unhandled exception: {}'.format(e))
            logger.exception("Unhandled exception:")


if __name__ == '__main__':
    main()
