import time
import random
import logging
from winrm import Session

from integrations.ad_integration.ad_common import AD
from integrations.ad_integration import read_ad_conf_settings

logger = logging.getLogger("AdReader")


# SKIP_BRUGERTYPE


class ADParameterReader(AD):

    def __init__(self, skip_school=False):
        super().__init__()

        self.all_settings = read_ad_conf_settings.read_settings()
        if skip_school:
            self.all_settings['school']['read_school'] = False

        if self.all_settings['global']['winrm_host']:
            self.session = Session(
                'http://{}:5985/wsman'.format(
                    self.all_settings['global']['winrm_host']
                ),
                transport='kerberos',
                auth=(None, None)
            )
        else:
            self.session = None
        self.results = {}

    def read_encoding(self):
        """
        Read the character encoding of the Power Shell session.
        """
        ps_script = "$OutputEncoding | ConvertTo-Json"
        response = self._run_ps_script(ps_script)
        return response

    def read_it_all(self, school=False):
        # TODO: Contains duplicated code
        settings = self._get_setting(school)
        bp = self._ps_boiler_plate(school)
        get_command = "get-aduser -Filter '*'"

        server_string = ''
        if self.all_settings['global'].get('servers') is not None:
            server_string = ' -Server {} '.format(
                random.choice(self.all_settings['global']['servers'])
            )

        command_end = (' | ConvertTo-Json  | ' +
                       ' % {$_.replace("ø","&oslash;")} | ' +
                       '% {$_.replace("Ø","&Oslash;")} ')

        ps_script = (
            self._build_user_credential(school) +
            get_command +
            server_string +
            bp['complete'] +
            self._properties(school) +
            bp['get_ad_object'] +
            command_end
        )

        response = self._run_ps_script(ps_script)
        return response

    def uncached_read_user(self, user=None, cpr=None):
        # Bug, currently this will not work directly with the school domain. Users
        # will be cached (and can be read by read_user) but will not be returned
        # directly by this function
        logger.debug('Uncached AD read, user {}, cpr {}'.format(user, cpr))

        if self.all_settings['school']['read_school']:
            settings = self._get_setting(school=True)
            # response = self._get_from_ad(user=user, cpr=cpr, school=True)
            response = self.get_from_ad(user=user, cpr=cpr, school=True,
                                        server=settings['server'])
            for current_user in response:
                job_title = current_user.get('Title')
                if job_title and job_title.find('FRATR') == 0:
                    continue  # These are users that has left

                if 'mail' in current_user:
                    current_user['EmailAddress'] = current_user['mail']
                    del current_user['mail']
                school_cpr = current_user[settings['cpr_field']].replace(
                    settings['cpr_separator'], '')
                self.results[school_cpr] = current_user
                self.results[current_user['SamAccountName']] = current_user

        response = self.get_from_ad(user=user, cpr=cpr, school=False)
        current_user = {}
        try:
            for current_user in response:
                settings = self._get_setting(school=False)
                job_title = current_user.get('Title')
                if job_title and job_title.find('FRATR') == 0:
                    continue  # These are users that has left

                brugertype = current_user.get('xBrugertype')
                if brugertype and brugertype.find('Medarbejder') == -1:
                    continue
                if not current_user:
                    current_user = {}

                cpr = current_user[settings['cpr_field']].replace(
                    settings['cpr_separator'], '')
                self.results[current_user['SamAccountName']] = current_user
                if current_user['SamAccountName'].startswith(settings['sam_filter']):
                    self.results[cpr] = current_user
            return current_user
        except Exception:
            logger.error('Response from uncached_read_user: {}'.format(response))
            raise

    def cache_all(self):
        logger.info('Caching all users')
        t = time.time()
        for i in range(1, 32):
            day = str(i).zfill(2)
            self.uncached_read_user(cpr='{}*'.format(day))
            logger.debug(len(self.results))
            logger.debug('Read time: {}'.format(time.time() - t))

    def read_user(self, user=None, cpr=None, cache_only=False):
        """
        Read all properties of an AD user. The user can be retrived either by cpr
        or by AD user name.
        :param user: The AD username to retrive.
        :param cpr: cpr number of the user to retrive.
        :return: All properties listed in AD for the user.
        """
        logger.debug('Cached AD read, user {}, cpr {}'.format(user, cpr))
        if (not cpr) and (not user):
            return

        if user:
            dict_key = user
            if user in self.results:
                return self.results[user]

        if cpr:
            dict_key = cpr
            if cpr in self.results:
                return self.results[cpr]

        if cache_only:
            return {}

        # Poulate self.results:
        self.uncached_read_user(user=user, cpr=cpr)

        logger.debug('Returned info for {}: {}'.format(
            dict_key, self.results.get(dict_key, {})))
        return self.results.get(dict_key, {})


if __name__ == '__main__':
    ad_reader = ADParameterReader()

    everything = ad_reader.read_it_all()

    for user in everything:
        print('Name: {}, Sam: {}, Manager: {}'.format(
            user['Name'], user['SamAccountName'], user.get('Manager')))
        # if user['SamAccountName'] == 'JSTEH':
        #    for key in sorted(user.keys()):
        #        print('{}: {}'.format(key, user[key]))
