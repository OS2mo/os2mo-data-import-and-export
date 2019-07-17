import time
import json
import pickle
import logging
import hashlib
import read_ad_conf_settings
from pathlib import Path
from winrm import Session
from winrm.exceptions import WinRMTransportError

logger = logging.getLogger("AdReader")


# SKIP_BRUGERTYPE


class ADParameterReader(object):

    def __init__(self):
        self.all_settings = read_ad_conf_settings.read_settings_from_env()
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

    def _run_ps_script(self, ps_script):
        """
        Run a power shell script and return the result. If it fails, the
        error is returned in its raw form.
        :param ps_script: The power shell script to run.
        :return: A dictionary with the returned parameters.
        """
        response = {}
        if not self.session:
            return response

        retries = 0
        try_again = True
        while try_again and retries < 10:
            try:
                r = self.session.run_ps(ps_script)
                try_again = False
            except WinRMTransportError:
                logger.error('AD read error: {}'.format(retries))
                time.sleep(5)
                retries += 1
                # The existing session is now dead, create a new.
                self.session = Session(
                    'http://{}:5985/wsman'.format(
                        self.all_settings['global']['winrm_host']
                    ),
                    transport='kerberos',
                    auth=(None, None)
                )

        # TODO: We will need better error handling than this.
        assert(retries < 10)

        if r.status_code == 0:
            if r.std_out:
                response = json.loads(r.std_out.decode('Latin-1'))
        else:
            response = r.std_err
        return response

    def _get_setting(self, school):
        if school and not self.all_settings['school']['read_school']:
            msg = 'Trying to access school without credentials'
            logger.error(msg)
            raise Exception(msg)
        if school:
            setting = 'school'
        else:
            setting = 'primary'
        return self.all_settings[setting]

    def _build_user_credential(self, school=False):
        """
        Build the commonn set of Power Shell commands that is needed to
        run the AD commands.
        :return: A suitable string to prepend to AD commands.
        """

        credential_template = """
        $User = "{}"
        $PWord = ConvertTo-SecureString –String "{}" –AsPlainText -Force
        $TypeName = "System.Management.Automation.PSCredential"
        $UserCredential = New-Object –TypeName $TypeName –ArgumentList $User, $PWord
        """
        settings = self._get_setting(school)
        user_credential = credential_template.format(settings['system_user'],
                                                     settings['password'])
        return user_credential

    def read_encoding(self):
        """
        Read the character encoding of the Power Shell session.
        """
        ps_script = "$OutputEncoding | ConvertTo-Json"
        response = self._run_ps_script(ps_script)
        return response

    def _get_from_ad(self, user=None, cpr=None, school=False):
        """
        Read all properties of an AD user. The user can be retrived either by cpr
        or by AD user name.
        :param user: The AD username to retrive.
        :param cpr: cpr number of the user to retrive.
        :return: All properties listed in AD for the user.
        """
        settings = self._get_setting(school)

        if user:
            dict_key = user
            ps_template = "get-aduser {} "
            get_command = ps_template.format(user)

        if cpr:
            dict_key = cpr
            # Here we should strongly consider to strip part of the cpr to
            # get more users at the same time to increase performance.
            # Lookup time is only very slightly dependant on the number
            # of results.
            field = settings['cpr_field']
            ps_template = "get-aduser -Filter '" + field + " -like \"{}\"'"

        get_command = ps_template.format(dict_key)

        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        search_base = ' -SearchBase "{}" '.format(settings['search_base'])
        credentials = ' -Credential $usercredential'

        get_ad_object = ''
        if settings['get_ad_object']:
            get_ad_object = ' | Get-ADObject'

        # properties = ' -Properties *'
        properties = ' -Properties '
        for item in settings['properties']:
            properties += item + ','
        properties = properties[:-1] + ' '  # Remove trailing comma, add space

        command_end = ' | ConvertTo-Json'

        ps_script = (
            self._build_user_credential(school) +
            get_command +
            server +
            search_base +
            credentials +
            get_ad_object +
            properties +
            command_end
        )

        response = self._run_ps_script(ps_script)

        if not response:
            return_val = []
        else:
            if not isinstance(response, list):
                return_val = [response]
            else:
                return_val = response
        return return_val

    def uncached_read_user(self, user=None, cpr=None):
        logger.debug('Uncached AD read, user {}, cpr {}'.format(user, cpr))

        if self.all_settings['school']['read_school']:
            settings = self._get_setting(school=True)
            response = self._get_from_ad(user=user, cpr=cpr, school=True)
            for current_user in response:
                job_title = current_user.get('Title')
                if job_title and job_title.find('FRATR') == 0:
                    continue  # These are users that has left

                if 'mail' in current_user:
                    current_user['EmailAddress'] = current_user['mail']
                    del current_user['mail']
                cpr = current_user[settings['cpr_field']].replace('-', '')
                self.results[cpr] = current_user
                self.results[current_user['SamAccountName']] = current_user

        response = self._get_from_ad(user=user, cpr=cpr, school=False)

        current_user = {}
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

            cpr = current_user[settings['cpr_field']].replace('-', '')
            self.results[cpr] = current_user
            self.results[current_user['SamAccountName']] = current_user
        return current_user

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

        m = hashlib.sha256()
        m.update(dict_key.encode())
        cache_file = Path('ad_' + m.hexdigest() + '.p')

        if cache_file.is_file():
            with open(str(cache_file), 'rb') as f:
                logger.debug('{} was found in AD cache'.format(dict_key))
                response = pickle.load(f)
                if not response:
                    response = {}
                self.results[dict_key] = response
        else:
            logger.debug('{} was not found in AD cache'.format(dict_key))
            response = self.uncached_read_user(user=user, cpr=cpr)
            with open(str(cache_file), 'wb') as f:
                pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

        logger.debug('Returned info for {}'.format(dict_key))
        logger.debug(self.results.get(dict_key, {}))
        return self.results.get(dict_key, {})


if __name__ == '__main__':
    ad_reader = ADParameterReader()
    # print(ad_reader.read_encoding())
    ad_reader.uncached_read_user(cpr='1911*')
    for person in ad_reader.results:
        print(ad_reader.results[person])
        print()
    # user = ad_reader.read_user(user='konroje')
    # print(user)
