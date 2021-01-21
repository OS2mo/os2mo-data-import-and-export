import argparse
import json
import logging
import pathlib
import random

import requests
from ad_common import AD
from os2mo_helpers.mora_helpers import MoraHelper
from tqdm import tqdm

from integrations.ad_integration import ad_logger, ad_reader

LOG_FILE = 'sync_mo_uuid_to_ad.log'
logger = logging.getLogger('MoUuidAdSync')

class SyncMoUuidToAd(AD):
    """
    Small tool to help the development of AD write test.
    Walks through all users in AD, search in MO by cpr and writes the MO
    uuid on the users AD account.
    """

    def __init__(self):
        ad_logger.start_logging(LOG_FILE)
        super().__init__()
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.helper = MoraHelper(hostname=self.all_settings['global']['mora.base'],
                                 use_cache=False)
        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        self.reader = ad_reader.ADParameterReader()

        self.stats = {
            'attempted_users': 0,
            'updated': 0,
            'user_not_in_mo': 0,
        }

    def _search_mo_cpr(self, cpr):
        # Todo, add this to MoraHelper.
        # skriv om til at bruge cachen
        user = {'items': []}
        if cpr is not None:
            user = self.helper._mo_lookup(self.org_uuid, 'o/{}/e?query=' + cpr)
        if not len(user['items']) == 1:
            uuid = None
        else:
            uuid = user['items'][0]['uuid']
        return uuid

    def perform_sync(self, all_users=[]):
        if len(all_users) == 0:
            all_users = self.reader.read_it_all()

        logger.info('Will now attempt to sync {} users'.format(len(all_users)))

        for user in tqdm(all_users):
            self.stats['attempted_users'] += 1
            cpr = user.get(self.all_settings['primary']['cpr_field'])
            separator = self.all_settings['primary'].get('cpr_separator', '')
            if separator:
                cpr = cpr.replace(separator, '')
            mo_uuid = self._search_mo_cpr(cpr)
            if not mo_uuid:
                self.stats['user_not_in_mo'] += 1
                continue

            expected_mo_uuid = user.get(
                self.settings['integrations.ad.write.uuid_field'])
            if expected_mo_uuid == mo_uuid:
                logger.debug('uuid for {} correct in AD'.format(user))
                continue

            server_string = ''
            if self.all_settings['global'].get('servers'):
                server_string = ' -Server {} '.format(
                    random.choice(self.all_settings['global'].get('servers'))
                )

            logger.debug('Need to sync {}'.format(user))
            ps_script = (
                self._build_user_credential() +
                "Get-ADUser " + server_string + " -Filter 'SamAccountName -eq \"" +
                user['SamAccountName'] + "\"' -Credential $usercredential | " +
                " Set-ADUser -Credential $usercredential " +
                " -Replace @{\"" +
                self.settings['integrations.ad.write.uuid_field'] +
                "\"=\"" + mo_uuid + "\"} " + server_string
            )
            logger.debug('PS-script: {}'.format(ps_script))
            response = self._run_ps_script(ps_script)
            logger.debug('Response: {}'.format(response))
            if response: 
                msg= 'Unexpected response: {}'.format(response)
                logger.exception(msg)
                raise Exception(msg)
            self.stats['updated'] += 1
        print(self.stats)
        logger.info(self.stats)

    def sync_one(self, cprno):
        user = self.reader.read_user(cpr=cprno)
        if user:
            self.perform_sync([user])
        else:
            msg = "User not found"
            logger.exception(msg)
            raise Exception(msg)

    def cli(self):
        """
        Command line interface for the AD writer class.
        """
        parser = argparse.ArgumentParser(description='SyncMoUuidToAd')
        parser.add_argument('--debug', default="false", help="(true/false) Set logging level to debug, default is false")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--sync-all', action='store_true')
        group.add_argument('--sync-cpr', nargs=1, required=False)
        args = vars(parser.parse_args())
        logger.level = logging.INFO
        if args.get('debug').lower() == "true":
            logger.level=logging.DEBUG
        logger.info(args)

        if args.get('sync_all'):
            self.perform_sync()
        if args.get('sync_cpr'):
            self.sync_one(args["sync_cpr"][0])
            
        logger.info("Sync done")

if __name__ == '__main__':
    sync = SyncMoUuidToAd()
    sync.cli()
