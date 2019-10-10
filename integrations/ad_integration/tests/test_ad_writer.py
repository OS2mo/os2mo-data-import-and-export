# import mock
import unittest

from ad_writer import ADWriter

test_responses = {}


def read_mo_info(uuid, read_manager=True):
    mo_values = {
        'name': ('Martin Lee', 'Gore'),
        'employment_number': '101',
        'uuid': '7ccbd9aa-gd60-4fa1-4571-0e6f41f6ebc0',
        'end_date': '2089-11-11',
        'cpr': '1122334455',
        'title': 'Musiker',
        'unit': 'Enhed',
        'unit_uuid': '101bd9aa-0101-0101-0101-0e6f41f6ebc0',
        'location': 'Kommune\\Forvalting\\Enhed\\',
        'forvaltning': 'Beskæftigelse, Økonomi & Personale',
        'manager_name': None,
        'manager_sam': None,
        'manager_email': None
    }
    if read_manager:
        mo_values['manager_name'] = 'Daniel Miller'
        mo_values['manager_sam'] = 'DMILL'
        mo_values['manager_email'] = 'dmill@spirit.co.uk'

    return mo_values


def get_from_ad(user=None, cpr=None):
    return {}


def return_ps_script(ps_script):
    test_responses['ps_script'] = ps_script
    return {}


class TestAdWriter(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.ad_writer = ADWriter()
        self.ad_writer.read_ad_informaion_from_mo = read_mo_info
        self.ad_writer._run_ps_script = return_ps_script
        self.ad_writer.get_from_ad = get_from_ad

    def _read_non_common_line(self):
        script = test_responses['ps_script']
        script = script.strip()
        lines = script.split('\n')
        line = lines[4].strip()  # First four lines are common to all scripts
        return line

    def test_common_ps_code(self):
        """
        Test the first four lines, that are identical for all writes to the AD.
        Create_user is used as a test-case to provoke the creation of a PS script,
        but only the common top of the code is actually tested.
        """
        self.ad_writer.create_user(mo_uuid='0', create_manager=False)
        create_script = test_responses['ps_script']
        create_script = create_script.strip()

        lines = create_script.split('\n')

        self.assertTrue(len(lines) == 5)
        self.assertTrue(lines[0].find("$User = ") == 0)

        line_one = '$User = '
        line_two = '$PWord = ConvertTo-SecureString –String'
        line_three = '$TypeName = "System.Management.Automation.PSCredential"'
        line_four = ('$UserCredential = New-Object ' +
                     '–TypeName $TypeName –ArgumentList $User, $PWord')

        self.assertTrue(lines[0].strip().find(line_one) == 0)
        self.assertTrue(lines[1].strip().find(line_two) == 0)
        self.assertTrue(lines[2].strip() == line_three)
        self.assertTrue(lines[3].strip() == line_four)

    def test_create_user_without_manager(self):
        self.ad_writer.create_user(mo_uuid='0', create_manager=False)
        create_script = test_responses['ps_script']
        create_script = create_script.strip()

        lines = create_script.split('\n')
        line = lines[4]  # First four lines are common to all scripts

        expected_content = [
            'New-ADUser',
            '-Name "Martin Lee Gore - MLEGO"',
            '-Displayname "Martin Lee Gore"',
            '-GivenName "Martin Lee"',
            '-SurName "Martin Lee Gore"',
            '-SamAccountName "MLEGO"',
            '-EmployeeNumber "101"',
            '-Credential $usercredential',
            '"xAutoritativForvaltning"="Beskæftigelse, Økonomi og Personale"',
            '"xAutoritativOrg"="Kommune\\Forvalting\\Enhed\\"',
            ';"xSTSBrugerUUID"="7ccbd9aa-gd60-4fa1-4571-0e6f41f6ebc0"',
            '"xAttrCPR"="1122334455"',
            '-Path "OU'
        ]

        for content in expected_content:
            self.assertTrue(line.find(content) > -1)

    def test_add_manager(self):
        user = self.ad_writer.read_ad_informaion_from_mo(uuid='0', read_manager=True)

        self.ad_writer.add_manager_to_user('MGORE', manager_sam=user['manager_sam'])
        manager_script = test_responses['ps_script']
        manager_script = manager_script.strip()
        lines = manager_script.split('\n')
        line = lines[4].strip()  # First four lines are common to all scripts

        expected_line = ("Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"'" +
                         " -Credential $usercredential |Set-ADUser -Manager DMILL" +
                         " -Credential $usercredential")
        self.assertTrue(line == expected_line)

    def test_set_password(self):
        password = 'password'
        self.ad_writer.set_user_password('MGORE', password)
        line = self._read_non_common_line()

        expected_line = (
            "Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"' -Credential" +
            " $usercredential |Set-ADAccountPassword -Reset -NewPassword" +
            " (ConvertTo-SecureString -AsPlainText \"{}\" -Force)" +
            " -Credential $usercredential"
        ).format(password)
        self.assertTrue(line == expected_line)

    def test_sync(self):
        user_ad_info = {
            'SamAccountName': 'MGORE'
        }
        self.ad_writer.sync_user(mo_uuid='0', user_ad_info=user_ad_info,
                                 sync_manager=False)
        line = self._read_non_common_line()

        expected_content = [
            "Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"' -Credential $usercredential |",
            'Set-ADUser -Credential $usercredential -Displayname "Martin Lee Gore"',
            '-GivenName "Martin Lee" -SurName "Martin Lee Gore" -EmployeeNumber \"101\"',
            "-Replace @{",
            '"xAutoritativForvaltning"="Beskæftigelse, Økonomi og Personale"',
            '"xAutoritativOrg"="Kommune\\Forvalting\\Enhed\\"'
        ]

        for content in expected_content:
            self.assertTrue(line.find(content) > -1)
