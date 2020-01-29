import time
import json
import logging

from winrm import Session
from winrm.exceptions import WinRMTransportError

from integrations.ad_integration import ad_exceptions
from integrations.ad_integration import read_ad_conf_settings

logger = logging.getLogger('AdCommon')
# Is this universal?
ENCODING = 'cp850'


class AD(object):
    def __init__(self):
        self.all_settings = read_ad_conf_settings.read_settings()
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
        logger.debug('Attempting to run script: {}'.format(ps_script))
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
                output = r.std_out.decode(ENCODING)
                output = output.replace('&Oslash;', 'Ø')
                output = output.replace('&oslash;', 'ø')
                response = json.loads(output)
        else:
            response = r.std_err
        return response

    def _ps_boiler_plate(self, school):
        """
        Boiler plate that needs to go into all PowerShell code.
        """
        settings = self._get_setting(school)

        # This is most likely never neeed.
        server = ''
        if settings['server']:
            server = ' -Server {} '.format(settings['server'])

        # TODO: Are these really different?
        path = ''
        search_base = ''
        if settings['search_base']:
            path = ' -Path "{}" '.format(settings['search_base'])
            search_base = ' -SearchBase "{}" '.format(settings['search_base'])

        get_ad_object = ''
        # TODO: When do we need this?
        # if settings['get_ad_object']:
        #    get_ad_object = ' | Get-ADObject'

        credentials = ' -Credential $usercredential'

        boiler_plate = {
            'server': server,
            'path': path,
            'get_ad_object': get_ad_object,
            'search_base': search_base,
            'credentials': credentials,
            'complete': server + search_base + credentials
        }
        return boiler_plate

    def remove_redundant(self, text):
        text = text.replace('\n', '')
        text = text.replace('\r', '')
        while text.find('  ') > -1:
            text = text.replace('  ', ' ')
        return text

    def _build_ps(self, ps_script, school, format_rules):
        """
        Return the standard code need to execute a power shell script from a
        template.
        """
        formatted_script = ps_script.format(**format_rules)
        finished_ps_script = (
            self._build_user_credential(school) +
            self.remove_redundant(formatted_script)
        )
        return finished_ps_script

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

    def _properties(self, school):
        settings = self._get_setting(school)
        # properties = ' -Properties *'
        properties = ' -Properties '
        for item in settings['properties']:
            properties += item + ','
        properties = properties[:-1] + ' '  # Remove trailing comma, add space
        return properties

    def _find_unique_user(self, cpr):
        """
        Find a unique AD account from cpr, otherwise raise an exception.
        """
        # TODO: Handle school
        user_ad_info = self.get_from_ad(cpr=cpr)

        if len(user_ad_info) == 1:
            user_sam = user_ad_info[0]['SamAccountName']
        elif len(user_ad_info) == 0:
            msg = 'Found no account for {}'.format(cpr)
            logger.error(msg)
            raise ad_exceptions.UserNotFoundException(msg)
        else:
            msg = 'Too many SamAccounts for {}'.format(cpr)
            logger.error(msg)
            raise ad_exceptions.CprNotNotUnique(msg)
        return user_sam

    def get_from_ad(self, user=None, cpr=None, school=False, server=None):
        """
        Read all properties of an AD user. The user can be retrived either by cpr
        or by AD user name.
        :param user: The SamAccountName to retrive.
        :param cpr: cpr number of the user to retrive.
        :param server: Add an explcit server to the query. Mostly needed to check
        if replication is finished.
        :return: All properties listed in AD for the user.
        """
        settings = self._get_setting(school)
        bp = self._ps_boiler_plate(school)

        if user:
            dict_key = user
            ps_template = "get-aduser -Filter 'SamAccountName -eq \"{}\"' "

        if cpr:
            dict_key = cpr
            field = settings['cpr_field']
            ps_template = "get-aduser -Filter '" + field + " -like \"{}\"'"

        get_command = ps_template.format(dict_key)

        server_string = ''
        if server:
            server_string = ' -Server {}'.format(server)

        command_end = (' | ConvertTo-Json  | ' +
                       ' % {$_.replace("ø","&oslash;")} | ' +
                       '% {$_.replace("Ø","&Oslash;")} ')

        ps_script = (
            self._build_user_credential(school) +
            get_command +
            server_string +
            bp['complete'] +
            self._properties(school) +
            # bp['get_ad_object'] +
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
