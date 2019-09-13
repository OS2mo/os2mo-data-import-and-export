import time
import json
import logging

from winrm import Session
from winrm.exceptions import WinRMTransportError
import read_ad_conf_settings

# TODO: How should we name the loggers?!?
logger = logging.getLogger("AdCommon")

# Is this universal?
ENCODING = 'cp850'


class AD(object):
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
                response = json.loads(r.std_out.decode(ENCODING))
        else:
            response = r.std_err
        return response

    def _ps_boiler_plate(self, school):
        """
        Boiler plate that needs to go into all PowerShell code.
        """
        settings = self._get_setting(school)
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
        if settings['get_ad_object']:
            get_ad_object = ' | Get-ADObject'
            
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

    def _build_ps(self, ps_script, school, format_rules):
        """
        Return the standard code need to execute a power shell script from a
        template.
        """
        formatted_script = ps_script.format(**format_rules)
        finished_ps_script = (
            self._build_user_credential(school) +
            remove_redundant(formatted_script)
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

    def get_from_ad(self, user=None, cpr=None, school=False):
        """
        Read all properties of an AD user. The user can be retrived either by cpr
        or by AD user name.
        :param user: The SamAccountName to retrive.
        :param cpr: cpr number of the user to retrive.
        :return: All properties listed in AD for the user.
        """
        settings = self._get_setting(school)

        if user:
            dict_key = user
            ps_template = "get-aduser -Filter 'SamAccountName -eq \"{}\"' "
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
