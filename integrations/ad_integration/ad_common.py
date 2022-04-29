import json
import logging
import random
import subprocess
import time
from typing import Dict
from typing import List
from typing import Optional

import more_itertools

try:
    from winrm import Session
    from winrm.exceptions import WinRMTransportError
    from winrm.vendor.requests_kerberos.exceptions import KerberosExchangeError
except ImportError:
    pass

from .ad_exceptions import CommandFailure
from .ad_exceptions import CprNotFoundInADException
from .ad_exceptions import CprNotNotUnique
from .read_ad_conf_settings import read_settings

logger = logging.getLogger("AdCommon")


ADUser = Dict[str, str]


def ad_minify(text):
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    while text.find("  ") > -1:
        text = text.replace("  ", " ")
    return text


def generate_ntlm_session(hostname, system_user, password):
    """Method to create a ntlm session for running powershell scripts.

    Returns:
        winrm.Session
    """
    session = Session(
        "https://{}:5986/wsman".format(hostname),
        transport="ntlm",
        auth=(system_user, password),
        server_cert_validation="ignore",
    )
    return session


class ReauthenticatingKerberosSession:
    """
    Wrapper around WinRM Session object that automatically tries to generate
    a new Kerberos token and session if authentication fails
    """

    def _generate_kerberos_ticket(self):
        """
        Tries to generate a new Kerberos ticket, through a call to kinit
        Raises an exception if the subprocess has non-zero exit code
        """
        cmd = ["kinit", self._username]
        try:
            subprocess.run(
                cmd,
                check=True,
                input=self._password.encode(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            print(e.stderr.decode("utf-8"))
            raise

    def _create_new_session(self):
        """
        Generate new internal session

        XXX: Session is not properly cleaned up when a command fails
             so we have to create a new internal session object each time
        """
        self._session = Session(
            target=self._target, transport="kerberos", auth=(None, None)
        )

    def __init__(self, target: str, username: str, password: str):
        self._username = username
        self._password = password
        self._target = target
        self._create_new_session()

    def run_cmd(self, *args, **kwargs):
        try:
            rs = self._session.run_cmd(*args, **kwargs)
        except KerberosExchangeError:
            self._generate_kerberos_ticket()
            self._create_new_session()
            rs = self._session.run_cmd(*args, **kwargs)
        return rs

    def run_ps(self, *args, **kwargs):
        try:
            rs = self._session.run_ps(*args, **kwargs)
        except KerberosExchangeError:
            self._generate_kerberos_ticket()
            self._create_new_session()
            rs = self._session.run_ps(*args, **kwargs)
        return rs


def generate_kerberos_session(hostname, username=None, password=None):
    """
    Method to create a kerberos session for running powershell scripts.

    Returned object will have same public interface as WinRM Session, despite
    not inheriting from it

    Returns:
        ReauthenticatingKerberosSession
    """
    session = ReauthenticatingKerberosSession(
        "http://{}:5985/wsman".format(hostname),
        username=username,
        password=password,
    )
    return session


class AD:
    _encoding = "utf-8"

    def __init__(self, all_settings=None, index=0, **kwargs):
        self.all_settings = all_settings
        if self.all_settings is None:
            self.all_settings = read_settings(index=index)
        self.session = self._create_session()
        self.retry_exceptions = self._get_retry_exceptions()
        self.results = {}

    def _get_retry_exceptions(self):
        """Tuple of exceptions which should trigger retrying create_session."""
        return (WinRMTransportError,)

    def _create_session(self):
        """Method to create a session for running powershell scripts.

        The returned object should have a run_ps method, which consumes a
        powershell script, and returns a status object.

        The status object should have a status_code, std_out and std_err
        attribute, containing the result from executing the powershell script.

        Returns:
            winrm.Session
        """

        all_settings = self.all_settings

        if all_settings["primary"]["method"] == "ntlm":
            session = generate_ntlm_session(
                all_settings["global"]["winrm_host"],
                all_settings["global"]["system_user"],
                all_settings["global"]["password"],
            )
        elif all_settings["primary"]["method"] == "kerberos":
            session = generate_kerberos_session(
                all_settings["global"]["winrm_host"],
                all_settings["global"]["system_user"],
                all_settings["global"]["password"],
            )
        else:
            raise ValueError(
                "Unknown WinRM method: %r" % all_settings["primary"]["method"]
            )

        return session

    def _run_ps_script(self, ps_script):
        """Run a PowerShell script and return the result.

        If the script fails, a `CommandFailure` exception is raised.
        If the script output cannot be parsed as JSON, a `ValueError` exception
        is raised.

        :param ps_script: The PowerShell script to run.
        :return: A dictionary with the returned parameters.
        """

        encoding = self._ps_boiler_plate()["encoding"]
        if encoding not in ps_script:
            sep = "\n" if not ps_script.startswith("\n") else ""
            ps_script = f"{encoding}{sep}{ps_script}"

        logger.debug("Attempting to run script: {}".format(ps_script))
        response = {}
        if not self.session:
            return response

        retries = 0
        try_again = True
        while try_again and retries < 10:
            try:
                r = self.session.run_ps(ps_script)
                try_again = False
            except self.retry_exceptions:
                logger.error("AD read error: {}".format(retries))
                time.sleep(5)
                retries += 1
                # The existing session is now dead, create a new.
                self.session = self._create_session()

        # TODO: We will need better error handling than this.
        assert retries < 10

        if r.status_code != 0:
            raise CommandFailure(r.std_err)
        if not r.std_out:
            logger.warning("status_code=0 but no std_out")
            return {}
        return self._parse_ps_script_result(ps_script, r)

    def _parse_ps_script_result(self, script, response):
        output = response.std_out
        if isinstance(output, bytes):
            output = output.decode(self._encoding)

        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            msg = "Could not parse JSON response, error in lines:\n%s\nscript:\n%s"
            line_sep = b"\n" if isinstance(response.std_out, bytes) else "\n"
            invalid_bound = slice(max(0, exc.lineno - 1), exc.lineno + 1)
            invalid_lines = response.std_out.splitlines()[invalid_bound]
            invalid_lines = line_sep.join(invalid_lines)
            logger.error(msg, invalid_lines, script)
            raise ValueError(msg % (invalid_lines, script))

    def _ps_boiler_plate(self):
        """
        Boiler plate that needs to go into all PowerShell code.
        """
        settings = self._get_setting()

        # TODO: Are these really different?
        path = ""
        search_base = ""
        if settings["search_base"]:
            path = ' -Path "{}" '.format(settings["search_base"])
            search_base = ' -SearchBase "{}" '.format(settings["search_base"])

        credentials = " -Credential $usercredential"

        boiler_plate = {
            "encoding": "[Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8",
            "path": path,
            "search_base": search_base,
            "credentials": credentials,
            "complete": search_base + credentials,
        }
        return boiler_plate

    def remove_redundant(self, text):
        return ad_minify(text)

    def _build_ps(self, ps_script, format_rules):
        """
        Return the standard code need to execute a power shell script from a
        template.
        """
        formatted_script = ps_script.format(**format_rules)
        finished_ps_script = self._build_user_credential() + self.remove_redundant(
            formatted_script
        )
        return finished_ps_script

    def _get_setting(self):
        return self.all_settings["primary"]

    def _build_user_credential(self):
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
        settings = self._get_setting()
        user_credential = credential_template.format(
            settings["system_user"], settings["password"]
        )
        return user_credential

    def _properties(self):
        settings = self._get_setting()
        properties = " -Properties "
        for item in settings["properties"]:
            properties += item + ","
        properties = properties[:-1] + " "  # Remove trailing comma, add space
        return properties

    def _get_sam_from_ad_values(self, ad_values: ADUser) -> str:
        # `ad_values` contains a dict with data on *one* AD user
        return ad_values["SamAccountName"]

    def _find_ad_user(
        self, cpr: str, ad_dump: Optional[List[Dict[str, str]]] = None
    ) -> ADUser:
        """Find a unique AD account from cpr, otherwise raise an exception."""
        if ad_dump:
            cpr_field = self.all_settings["primary"]["cpr_field"]
            ad_users = filter(lambda ad_user: ad_user.get(cpr_field) == cpr, ad_dump)
        else:
            logger.debug("No AD information supplied, will look it up")
            ad_users = self.get_from_ad(cpr=cpr)

        return more_itertools.one(ad_users, CprNotFoundInADException, CprNotNotUnique)

    def get_from_ad(self, user=None, cpr=None, server=None):
        """
        Read all properties of an AD user. The user can be retrived either by cpr
        or by AD user name.

        Example:

            [
                {
                    'ObjectGUID': '7ccbd9aa-gd60-4fa1-4571-0e6f41f6ebc0',
                    'SID': {
                        'AccountDomainSid': {
                            'AccountDomainSid': 'S-x-x-xx-...',
                            'BinaryLength': 24,
                            'Value': 'S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx'
                        },
                        'BinaryLength': 28,
                        'Value': 'S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx-xxxxx'
                    },
                    'PropertyCount': 11,
                    'PropertyNames': [
                        'ObjectGUID',
                        'SID',
                        'DistinguishedName',
                        'Enabled',
                        'GivenName',
                        'Name',
                        'ObjectClass',
                        'SamAccountName',
                        'Surname',
                        'UserPrincipalName'
                        'extensionAttribute1',
                    ],
                    'DistinguishedName': 'CN=Martin Lee Gore,OU=...'
                    'Enabled': True,
                    'GivenName': 'Martin Lee',
                    'Name': 'Martin Lee Gore',
                    'ObjectClass': 'user',
                    'SamAccountName': 'mlego',
                    'Surname': 'Gore',
                    'UserPrincipalName': 'martinleegore@magenta.dk',
                    'extensionAttribute1': '1122334455',
                    'AddedProperties': [],
                    'ModifiedProperties': [],
                    'RemovedProperties': [],
                }
            ]

        :param user: The SamAccountName to retrive.
        :param cpr: cpr number of the user to retrive.
        :param server: Add an explcit server to the query. Mostly needed to check
        if replication is finished.
        :return: All properties listed in AD for the user.
        """
        settings = self._get_setting()
        bp = self._ps_boiler_plate()

        if user:
            ps_template = "Get-ADUser -Filter 'SamAccountName -eq \"{val}\"'"
            get_command = ps_template.format(val=user)

        if cpr:
            # For wild-card searches, we do a litteral search.
            if cpr.find("*") > -1:
                val = cpr
            else:
                # For direct cpr-search we obey the local separator setting.
                cpr_sep = settings["cpr_separator"]
                val = f"{cpr[0:6]}{cpr_sep}{cpr[6:10]}"

            field = settings["cpr_field"]
            operator = "-eq" if field.lower() == "objectguid" else "-like"
            ps_template = "Get-ADUser -Filter '{field} {operator} \"{val}\"'"
            get_command = ps_template.format(field=field, operator=operator, val=val)

        server_string = ""
        if server is not None:
            server_string = " -Server {}".format(server)
        elif self.all_settings["global"].get("servers"):
            server_string = " -Server {}".format(
                random.choice(self.all_settings["global"]["servers"])
            )

        ps_script = (
            self._ps_boiler_plate()["encoding"]
            + self._build_user_credential()
            + get_command
            + server_string
            + bp["complete"]
            + self._properties()
            + " | ConvertTo-Json"
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
