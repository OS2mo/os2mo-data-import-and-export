import json
import logging
import pathlib
import time

from exporters.sql_export.lora_cache import LoraCache

from .ad_exceptions import CprNotFoundInADException
from .ad_exceptions import CprNotNotUnique
from .ad_exceptions import ManagerNotUniqueFromCprException
from .ad_exceptions import UserNotFoundException
from .ad_logger import start_logging
from .ad_reader import ADParameterReader
from .ad_writer import ADWriter


LOG_FILE = 'mo_to_ad_sync.log'

logger = logging.getLogger('MoAdSync')


def main():
    t_start = time.time()
    start_logging(LOG_FILE)
    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    settings = json.loads(cfg_file.read_text())

    if settings['integrations.ad_writer.lora_speedup']:
        # Here we should activate read-only mode, actual state and
        # full history dumps needs to be in sync.

        # Full history does not calculate derived data, we must
        # fetch both kinds.
        lc = LoraCache(resolve_dar=False, full_history=False)
        lc.populate_cache(dry_run=False, skip_associations=True)
        lc.calculate_derived_unit_data()
        lc.calculate_primary_engagements()

        # Todo, in principle it should be possible to run with skip_past True
        # This is now fixed in a different branch, remember to update when
        # merged.
        lc_historic = LoraCache(resolve_dar=False, full_history=True,
                                skip_past=False)
        lc_historic.populate_cache(dry_run=False, skip_associations=True)
        # Here we should de-activate read-only mode
    else:
        lc = None
        lc_historic = None

    mo_uuid_field = settings['integrations.ad.write.uuid_field']

    reader = ADParameterReader()
    writer = ADWriter(lc=lc, lc_historic=lc_historic)

    all_users = reader.read_it_all()

    logger.info('Will now attempt to sync {} users'.format(len(all_users)))
    stats = {
        'attempted_users': 0,
        'fully_synced': 0,
        'nothing_to_edit': 0,
        'updated': 0,
        'no_manager': 0,
        'unknown_manager_failure': 0,
        'cpr_not_unique': 0,
        'user_not_in_mo': 0,
        'user_not_in_ad': 0,
        'critical_error': 0,
        'unknown_failed_sync': 0,
        'no_active_engagement': 0
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
            if response[0]:
                stats['fully_synced'] += 1
                if response[1] == 'Sync completed':
                    stats['updated'] += 1
                    if response[2] == False:
                        stats['no_manager'] += 1

                if response[1] == 'Nothing to edit':
                    stats['nothing_to_edit'] += 1
                    if response[2] == False:
                        stats['no_manager'] += 1

            else:
                if response[1] == 'No active engagments':
                    stats['no_active_engagement'] += 1
                else:
                    stats['unknown_failed_sync'] += 1
        except ManagerNotUniqueFromCprException:
            stats['unknown_manager_failure'] += 1
            msg = 'Did not find a unique manager for {}'.format(user[mo_uuid_field])
            logger.error(msg)
        except CprNotNotUnique:
            stats['cpr_not_unique'] += 1
            msg = 'User {} with uuid: {} has more than one AD account'
            logger.error(msg.format(user['Name'], user[mo_uuid_field]))
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
        except Exception as e:
            stats['critical_error'] += 1
            logger.error('Unhandled exception: {}'.format(e))
            logger.exception("Unhandled exception:")
            print('Unhandled exception: {}'.format(e))

        print('Sync time: {}'.format(time.time() - t))
    print()
    print('Total runtime: {}'.format(time.time() - t_start))
    print(stats)
    logger.info('Total runtime: {}'.format(time.time() - t_start))
    logger.info('Stats: {}'.format(stats))


if __name__ == '__main__':
    main()
