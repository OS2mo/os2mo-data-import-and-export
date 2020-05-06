import json
import random
import pathlib
import logging
import requests

from ad_common import AD
from integrations.ad_integration import ad_reader
from integrations.ad_integration import ad_logger
from os2mo_helpers.mora_helpers import MoraHelper

LOG_FILE = 'sync_mo_uuids_to_ad.log'
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

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
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
        user = {'items': []}
        if cpr is not None:
            user = self.helper._mo_lookup(self.org_uuid, 'o/{}/e?query=' + cpr)
        if not len(user['items']) == 1:
            uuid = None
        else:
            uuid = user['items'][0]['uuid']
        return uuid

    def perform_sync(self):
        all_users = self.reader.read_it_all()
        logger.info('Will now attempt to sync {} users'.format(len(all_users)))

        for user in all_users:
            self.stats['attempted_users'] += 1
            cpr = user.get(self.settings['integrations.ad.cpr_field'])
            separator = self.settings.get('integrations.ad.cpr_separator', '')
            if separator:
                cpr = cpr.replace(separator, '')
            mo_uuid = self._search_mo_cpr(cpr)
            if not mo_uuid:
                self.stats['user_not_in_mo'] += 1
                continue

            expected_mo_uuid = user.get(
                self.settings['integrations.ad.write.uuid_field'])
            if expected_mo_uuid == mo_uuid:
                logger.info('uuid for {} correct in AD'.format(user['DisplayName']))
                continue

            server_string = ''
            if self.settings.get('integrations.ad.write.servers') is not None:
                server_string = ' -Server {} '.format(
                    random.choice(self.settings['integrations.ad.write.servers'])
                )

            logger.info('Need to sync {}'.format(user['DisplayName']))
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
            self.stats['updated'] += 1
        print(self.stats)


if __name__ == '__main__':
    sync = SyncMoUuidToAd()
    sync.perform_sync()
