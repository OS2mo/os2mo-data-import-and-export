import os
import json
import time
import logging
import pathlib

from integrations.ad_integration import ad_reader
from integrations.ad_integration import ad_writer
from integrations.ad_integration import ad_logger
from integrations.ad_integration.ad_exceptions import UserNotFoundException
from integrations.ad_integration.ad_exceptions import CprNotFoundInADException
from integrations.ad_integration.ad_exceptions import ManagerNotUniqueFromCprException

from exporters.sql_export.lora_cache import LoraCache


LOG_FILE = 'mo_to_ad_sync.log'

# Notice!!!!!
# MO_UUID_FIELD = os.environ.get('AD_WRITE_UUID')

# Notice, logging os not working fully as expected.
logger = logging.getLogger('MoAdSync')


def main():
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    settings = json.loads(cfg_file.read_text())

    # if lora_speedup:
    lc = LoraCache(resolve_dar=True, full_history=False)
    lc.populate_cache(dry_run=True)
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()

    lc_historic = LoraCache(resolve_dar=False, full_history=True)
    lc_historic.populate_cache(dry_run=True)

    mo_uuid_field = settings['integrations.ad.write.uuid_field']

    ad_logger.start_logging(LOG_FILE)
    reader = ad_reader.ADParameterReader()
    writer = ad_writer.ADWriter(lc=lc, lc_historic=lc_historic)

    all_users = reader.read_it_all()

    logger.info('Will now attempt to sync {} users'.format(len(all_users)))
    stats = {
        'attempted_users': 0,
        'fully_synced': 0,
        'no_manager': 0,
        'user_not_in_mo': 0,
        'user_not_in_ad': 0
    }
    for user in all_users:
        t = time.time()
        stats['attempted_users'] += 1

        if mo_uuid_field not in user:
            msg = 'User {} does not have a {} field - skipping'
            logger.info(msg.format(user['SamAccountName'], mo_uuid_field))
            continue
        msg = 'Now syncing: {}, {}'.format(user['SamAccountName'],
                                           user[mo_uuid_field])
        logger.info(msg)
        print(msg)
        try:
            response = writer.sync_user(user[mo_uuid_field], ad_dump=all_users)
            # response = writer.sync_user(user[mo_uuid_field], ad_dump=None)
            logger.debug('Respose to sync: {}'.format(response))
            stats['fully_synced'] += 1
        except ManagerNotUniqueFromCprException:
            stats['no_manager'] += 1
            msg = 'Did not find a unique manager for {}'.format(user[mo_uuid_field])
            logger.error(msg)
        except CprNotFoundInADException:
            stats['user_not_in_ad'] += 1
            msg = 'User {}, {} with uuid {} could not be found by cpr'
            logger.error(msg.format(user['SamAccountName'], user['Name'],
                                    user[mo_uuid_field]))
        except UserNotFoundException:
            stats['user_not_in_mo'] += 1
            msg = 'User {}, {} with uuid {} was not found i MO, unable to sync'
            logger.error(msg.format(user['SamAccountName'], user['Name'],
                                    user[mo_uuid_field]))
        #except Exception as e:
        #    logger.error('Unhandled exception: {}'.format(e))
        #    logger.exception("Unhandled exception:")
        #    print('Unhandled exception: {}'.format(e))

        print('Sync time: {}'.format(time.time() - t))
    print()
    print(stats)


if __name__ == '__main__':
    main()
