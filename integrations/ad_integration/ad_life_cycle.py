import json
import pathlib
import logging
import argparse

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.ad_integration import ad_reader
from integrations.ad_integration import ad_logger
from integrations.ad_integration import ad_writer
from exporters.sql_export.lora_cache import LoraCache

from integrations.ad_integration.ad_exceptions import NoPrimaryEngagementException


logger = logging.getLogger('CreateAdUsers')


class AdLifeCycle(object):
    def __init__(self, dry_run=False):
        logger.info('AD Sync Started')
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())
        self.roots = self.settings['integrations.ad.write.create_user_trees']

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        self.org = self.helper.read_organisation()

        self.ad_reader = ad_reader.ADParameterReader()

        self.stats = {
            'critical_errors': 0,
            'engagement_not_found': 0,
            'created_users': 0,
            'users': set()
        }
        logger.info('__init__() done')

    def _update_ad_and_lora_cache(self, dry_run=False):
        """
        Read all information from AD and LoRa.
        :param dry_run: If True, LoRa dump will be read from cache.
        """
        # This is a slow step (since ADWriter reads all SAM names in __init__
        self.ad_writer = ad_writer.ADWriter()

        logger.info('Retrive AD dump')
        self.ad_reader.cache_all()
        logger.info('Done with AD caching')

        logger.info('Retrive LoRa dump')
        self.lc = LoraCache(resolve_dar=False, full_history=False)
        self.lc.populate_cache(dry_run=dry_run, skip_associations=True)
        self.lc.calculate_primary_engagements()
        self.lc_historic = LoraCache(resolve_dar=False, full_history=True,
                                     skip_past=True)
        self.lc_historic.populate_cache(dry_run=dry_run, skip_associations=True)
        logger.info('Done')
        # This is a list of current and future engagemetns, sorted by user
        self.user_engagements = {}
        for eng in self.lc_historic.engagements.values():
            if eng[0]['user'] in self.user_engagements:
                self.user_engagements[eng[0]['user']].append(eng[0])
            else:
                self.user_engagements[eng[0]['user']] = [eng[0]]

    def _find_user_unit_tree(self, user):
        user_engagements = self.user_engagements[user['uuid']]
        for eng in user_engagements:
            logger.info('Now checkng: {}'.format(eng))
            if eng['uuid'] in self.lc.engagements:
                primary = self.lc.engagements[eng['uuid']][0]['primary_boolean']
                if primary:
                    logger.info('Primary found, now find org unit location')
                    unit_uuid = self.lc.engagements[eng['uuid']][0]['unit']
                    unit = self.lc.units[unit_uuid][0]
                    while True:
                        if unit['uuid'] in self.roots:
                            return True
                        if unit['parent'] is None:
                            return False
                        unit = self.lc.units[unit['parent']][0]
            else:
                logger.info('Future engagement, look in MO')
                # This is a future engagement, we accept that the LoRa cache will
                # not provide the answer and search in MO.
                mo_engagements = self.helper.read_user_engagement(
                    user['uuid'], read_all=True,
                    skip_past=True, calculate_primary=True
                )
                primary = None

                for mo_eng in mo_engagements:
                    if mo_eng['uuid'] == eng['uuid']:
                        primary = mo_eng['is_primary']
                        if not primary:
                            continue
                        logger.info('Found future primary: {}'.format(mo_eng))
                        unit = self.helper.read_ou(mo_eng['org_unit']['uuid'])
                        while True:
                            if unit['uuid'] in self.roots:
                                logger.info('')
                                return True
                            if unit['parent'] is None:
                                return False
                            unit = unit['parent']

                if not primary:
                    msg = 'Warning: Unable to find primary for {}!'
                    logger.warning(msg.format(eng['uuid']))
                    print(msg.format(eng['uuid']))
                    return False

    def disable_ad_accounts(self):
        i = 0
        for employee in self.lc.users.values():
            # print(employee)
            i = i + 1
            if i % 1000 == 0:
                print('Progress: {}/{}'.format(i, len(self.lc.users)))
            # logger.info('Start sync of {}'.format(employee['uuid']))
            cpr = employee['cpr']

            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            if not response:
                logger.info('User {} has not AD account'.format(employee))
                continue

            # Check the user has a valid engagement:
            if employee['uuid'] in self.user_engagements:
                logger.info('Tis user is active - do not touch')
                continue

            logger.info('This user has no active engagemens, we should disable')
            # This user has an AD account, but no engagements - disable
            sam = response['SamAccountName']
            print('Disable {}'.format(sam))
            self.ad_writer.enable_user(username=sam, enable=False)

    def create_ad_accounts(self):
        """
        Iterate over all users and create missing AD accounts
        """
        i = 0
        for employee in self.lc.users.values():
            logger.debug('Now testing: {}'.format(employee))
            i = i + 1
            if i % 1000 == 0:
                print('Progress: {}/{}'.format(i, len(self.lc.users)))
            # logger.info('Start sync of {}'.format(employee['uuid']))
            cpr = employee['cpr']

            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            if response:
                logger.info('User {} is already in AD'.format(employee))
                continue

            # Check the user has a valid engagement:
            if employee['uuid'] not in self.user_engagements:
                logger.info('User has no active engagements - skip')
                continue

            # Check if the user is in a create-user sub-tree
            create_account = self._find_user_unit_tree(employee)
            logger.info('Create account: {}'.format(create_account))

            if create_account:
                logger.info('Create account for {}'.format(employee))
                # Create user without manager to avoid risk of failing
                # if manager is not yet in AD. The manager will be attached
                # by the next round of sync.
                try:
                    status = self.ad_writer.create_user(employee['uuid'],
                                                        create_manager=False)
                    logger.info('New username: {}'.format(status[1]))
                    self.stats['created_users'] += 1
                    self.stats['users'].add(employee['uuid'])
                except NoPrimaryEngagementException:
                    logger.error('No engagment found!!!')
                    self.stats['engagement_not_found'] += 1
                except:
                    self.stats['critical_errors'] += 1
                    logger.exception('Unknown error!')

        logger.info('Stats: {}'.format(self.stats))
        self.stats['users'] = 'Written in log file'
        print(self.stats)

    def cli(self):
        parser = argparse.ArgumentParser(description='Create or disale users')

        parser.add_argument('--create-ad-accounts',  action='store_true')
        parser.add_argument('--disable-ad-accounts',  action='store_true')
        parser.add_argument('--use-cached-mo',  action='store_true',
                            help='Use cached LoRa data, AD WILL be updated')

        args = vars(parser.parse_args())

        logger.info('Starting with args: {}'.format(args))

        if not (args['create_ad_accounts'] or args['disable_ad_accounts']):
            print('At least one of create or disable commands must be given')
            exit()
        
        self._update_ad_and_lora_cache(dry_run=args['use_cached_mo'])

        if args['create_ad_accounts']:
            self.create_ad_accounts()

        if args['disable_ad_accounts']:
            self.disable_ad_accounts()


if __name__ == '__main__':
    ad_logger.start_logging('AD_life_cycle.log')
    sync = AdLifeCycle()
    sync.cli()
    # sync.create_ad_accounts()
    # sync.disable_ad_accounts()
