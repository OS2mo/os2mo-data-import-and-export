import time
import json
import logging

from winrm import Session
from winrm.exceptions import WinRMTransportError
import read_ad_conf_settings

# TODO: How should we name the loggers?!?
logger = logging.getLogger("AdCommon")


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
