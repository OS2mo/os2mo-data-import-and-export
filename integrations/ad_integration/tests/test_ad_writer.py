# TODO: Fix imports in module
import sys
from os.path import dirname

sys.path.append(dirname(__file__))
sys.path.append(dirname(__file__) + "/..")

from unittest import TestCase

from parameterized import parameterized


from test_utils import TestADWriterMixin, dict_modifier, mo_modifier


class TestADWriter(TestCase, TestADWriterMixin):
    def setUp(self):
        self._setup_adwriter()

    def _verify_identitical_common_code(
        self, num_expected_scripts, num_common_lines=5
    ):
        """Verify that common code in all scripts is identitical.

        I.e. that all scripts start with the same num_common_lines lines.
        """
        self.assertEqual(len(self.ad_writer.scripts), num_expected_scripts)
        # 1. Convert each script from a string into a list of strings (lines)
        lines = [x.split("\n") for x in self.ad_writer.scripts]
        self.assertGreaterEqual(len(lines[0]), num_common_lines)
        self.assertEqual(len(lines), num_expected_scripts)
        # 2. Get the common lines (first 4 lines) in each script
        common_lines = [x[:num_common_lines] for x in lines]
        self.assertEqual(len(common_lines[0]), num_common_lines)
        self.assertEqual(len(common_lines), num_expected_scripts)
        # 3. Zip the lines producing 4 'n' tuples, where 'n' is len(scripts).
        zip_lines = list(zip(*common_lines))
        self.assertEqual(len(zip_lines[0]), num_expected_scripts)
        self.assertEqual(len(zip_lines), num_common_lines)
        # Check that all zip_lines are identitical
        for zip_line in zip_lines:
            self.assertEqual(len(set(zip_line)), 1)
        # Return common code
        return common_lines[0]

    @parameterized.expand(
        [
            # Test without any changes to settings
            [dict_modifier({}),],
            # Test with overridden password
            [dict_modifier({"primary": {"password": "Password1"}}),],
            [dict_modifier({"primary": {"password": "Hunter2"}}),],
            # Test with overridden user
            [dict_modifier({"primary": {"system_user": "R2D2"}}),],
            [dict_modifier({"primary": {"system_user": "C-3PO"}}),],
        ]
    )
    def test_common_ps_code(self, settings_transformer):
        """Test ps_script common code (first five lines of each script).

        The common code lines are identical for all writes to the AD.
        This is verified by the 'verify_identitical_common_code' method.

        Create_user is used as a test-case to provoke the creation of a PS script,
        but only the common top of the code is actually tested.
        """
        self._setup_adwriter(settings_transformer)

        # Assert no scripts were produced from initializing ad_writer itself
        self.assertGreaterEqual(len(self.ad_writer.scripts), 0)

        # Expected outputs
        num_expected_scripts = 3

        # Run create user and fetch scripts
        uuid = "invalid-provided-and-accepted-due-to-mocking"
        self.ad_writer.create_user(mo_uuid=uuid, create_manager=False)
        # Check that scripts were produced
        self.assertEqual(len(self.ad_writer.scripts), num_expected_scripts)

        # Verify that the first 4 lines are identitical for all scripts
        common_ps = self._verify_identitical_common_code(num_expected_scripts)
        common_ps = [x.strip() for x in common_ps]
        expected_ps = [
            "",
            '$User = "' + self.settings["primary"]["system_user"] + '"',
            '$PWord = ConvertTo-SecureString –String "'
            + self.settings["primary"]["password"]
            + '" –AsPlainText -Force',
            '$TypeName = "System.Management.Automation.PSCredential"',
            "$UserCredential = New-Object –TypeName $TypeName –ArgumentList $User, $PWord",
        ]
        self.assertEqual(common_ps, expected_ps)

    # Jinja to determine if mo_values['employment_number'] is a prime number
    is_prime_jinja = """
        {#- Aliased to n for readability -#}
        {%- set n = mo_values['employment_number'] | int -%}
        {#- Negative numbers, zero and one are not primes -#}
        {%- if n <= 1 -%}
            false
        {%- else -%}
            {#- Using vars as dict allows settings by reference -#}
            {%- set vars = {'is_prime': True} -%}

            {#- Really we only need to check until sqrt(n) -#}
            {%- for i in range(2,n) -%}
                {#- We cannot use percentage for modulus -#}
                {%- set quotient = (n/i) | int -%}
                {%- set remainder = n - i*quotient -%}

                {#- If it divides without remainder, it is not prime -#}
                {%- if remainder == 0 -%}
                    {#- Update is_prime by reference -#}
                    {%- set _ = vars.update({'is_prime': False}) -%}
                {%- endif -%}
            {%- endfor -%}

            {#- Print whether we found a prime or not -#}
            {%- if vars['is_prime'] -%}
                true
            {%- else -%}
                false
            {%- endif -%}
        {%- endif -%}
    """

    @parameterized.expand(
        [
            # Test without any changes
            [dict_modifier({}), None, ""],
            # Test with new employment number
            [dict_modifier({}), mo_modifier({"employment_number": "42"}), ""],
            [dict_modifier({}), mo_modifier({"employment_number": "100"}), ""],
            # Test with added template_field
            # Simple field lookup
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "unit_user_key": "{{ mo_values['unit_user_key'] }}"
                        }
                    }
                ),
                None,
                '"unit_user_key"="Musik";',
            ],
            # Field lookup and processing
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "street_number": """
                                {%- set street = mo_values['unit_streetname'] -%}
                                {{ street.split(' ')[-1] }}
                                {#- Comment used to trim whitespace -#}
                            """
                        }
                    }
                ),
                None,
                '"street_number"="451";',
            ],
            # Test if employment number is prime
            # 101 is prime
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "employment_number_is_prime": is_prime_jinja
                        }
                    }
                ),
                mo_modifier({"employment_number": "101"}),
                '"employment_number_is_prime"="true";',
            ],
            # 100 is not prime
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "employment_number_is_prime": is_prime_jinja
                        }
                    }
                ),
                mo_modifier({"employment_number": "100"}),
                '"employment_number_is_prime"="false";',
            ],
            # 263 is prime
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "employment_number_is_prime": is_prime_jinja
                        }
                    }
                ),
                mo_modifier({"employment_number": "263"}),
                '"employment_number_is_prime"="true";',
            ],
            # 267 is not prime
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "employment_number_is_prime": is_prime_jinja
                        }
                    }
                ),
                mo_modifier({"employment_number": "267"}),
                '"employment_number_is_prime"="false";',
            ],
        ]
    )
    def test_user_create_custom_fields(
        self, settings_transformer, mo_transformer, expected
    ):
        """Test user create ps_script code.

        The common code is not tested.
        """
        self._setup_adwriter(settings_transformer, mo_transformer)
        # Assert no scripts were produced from initializing ad_writer itself
        self.assertGreaterEqual(len(self.ad_writer.scripts), 0)

        # Expected outputs
        num_expected_scripts = 3

        # Run create user and fetch scripts
        uuid = "invalid-provided-and-accepted-due-to-mocking"
        self.ad_writer.create_user(mo_uuid=uuid, create_manager=False)
        # Check that scripts were produced
        self.assertEqual(len(self.ad_writer.scripts), num_expected_scripts)

        # Verify that the first 4 lines are identitical for all scripts
        self._verify_identitical_common_code(num_expected_scripts)

        # Check that the create user ps looks good
        create_user_ps = self.ad_writer.scripts[2].split("\n")[5].strip()

        mo_values = self.ad_writer.read_ad_information_from_mo(uuid)
        expected_content = [
            "New-ADUser",
            '-Name "' + mo_values['full_name'] + " - " + mo_values['sam_account_name'] + '"',
            '-Displayname "' + mo_values['full_name'] + '"',
            '-GivenName "' + mo_values['name'][0] + '"',
            '-SurName "' + mo_values['name'][1] + '"',
            '-SamAccountName "' + mo_values['sam_account_name'] + '"',
            '-EmployeeNumber "' + mo_values["employment_number"] + '"',
            '-Credential "$usercredential"',
            '-UserPrincipalName "' + mo_values['sam_account_name'] + '@epn_end"',
            "-OtherAttributes",
            '"level2orgunit_field"="Ingen";',
            '"org_field"="Kommune\\Forvalting\\Enhed\\";',
            '"uuid_field"="' + mo_values['uuid'] + '";',
            '"cpr_field"="' + mo_values['cpr'][0:6] + 'ad_cpr_sep' + mo_values['cpr'][6:] + '"',
            '-Path "search_base"',
            expected,
        ]
        for content in expected_content:
            self.assertIn(content, create_user_ps)

    def test_user_edit(self):
        """Test user edit ps_script code.

        The common code is not tested.
        """
        # Assert no scripts were produced from initializing ad_writer itself
        self.assertGreaterEqual(len(self.ad_writer.scripts), 0)

        # Expected outputs
        num_expected_scripts = 1

        # Run create user and fetch scripts
        uuid = "invalid-provided-and-accepted-due-to-mocking"
        self.ad_writer.sync_user(mo_uuid=uuid, sync_manager=False)
        # Check that scripts were produced
        self.assertEqual(len(self.ad_writer.scripts), num_expected_scripts)

        # Verify that the first 4 lines are identitical for all scripts
        self._verify_identitical_common_code(num_expected_scripts)

        # Check that the create user ps looks good
        edit_user_ps = self.ad_writer.scripts[0].split("\n")[5].strip()
        mo_values = self.ad_writer.read_ad_information_from_mo(uuid)
        expected_content = [
            "Get-ADUser",
            "-Filter 'SamAccountName -eq \"" + mo_values['sam_account_name'] + "\"'",
            '-Credential "$usercredential"',
            "|",
            "Set-ADUser",
            '-Credential "$usercredential"',
            '-Displayname "' + mo_values['full_name'] + '"',
            '-GivenName "' + mo_values['name'][0] + '"',
            '-SurName "' + mo_values['name'][1] + '"',
            '-EmployeeNumber "' + mo_values['employment_number'] + '"',
            "-Replace",
            '"level2orgunit_field"="Ingen";',
            '"org_field"="Kommune\\Forvalting\\Enhed\\";',
        ]
        for content in expected_content:
            self.assertIn(content, edit_user_ps)


#    def test_add_manager(self):
#        user = self.ad_writer.read_ad_information_from_mo(uuid='0', read_manager=True)
#
#        self.ad_writer.add_manager_to_user('MGORE', manager_sam=user['manager_sam'])
#        manager_script = test_responses['ps_script']
#        manager_script = manager_script.strip()
#        lines = manager_script.split('\n')
#        line = lines[4].strip()  # First four lines are common to all scripts
#
#        expected_line = ("Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"'" +
#                         " -Credential $usercredential |Set-ADUser -Manager DMILL" +
#                         " -Credential $usercredential")
#        self.assertTrue(line == expected_line)
#
#    def test_set_password(self):
#        password = 'password'
#        self.ad_writer.set_user_password('MGORE', password)
#        line = self._read_non_common_line()
#
#        expected_line = (
#            "Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"' -Credential" +
#            " $usercredential |Set-ADAccountPassword -Reset -NewPassword" +
#            " (ConvertTo-SecureString -AsPlainText \"{}\" -Force)" +
#            " -Credential $usercredential"
#        ).format(password)
#        self.assertTrue(line == expected_line)
#
#    def test_sync(self):
#        user_ad_info = {
#            'SamAccountName': 'MGORE'
#        }
#        self.ad_writer.sync_user(mo_uuid='0', user_ad_info=user_ad_info,
#                                 sync_manager=False)
#        line = self._read_non_common_line()
#
#        expected_content = [
#            "Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"' -Credential $usercredential |",
#            'Set-ADUser -Credential $usercredential -Displayname "Martin Lee Gore"',
#            '-GivenName "Martin Lee" -SurName "Martin Lee Gore" -EmployeeNumber \"101\"',
#            "-Replace @{",
#            '"xAutoritativForvaltning"="Beskæftigelse, Økonomi og Personale"',
#            '"xAutoritativOrg"="Kommune\\Forvalting\\Enhed\\"'
#        ]
#
#        for content in expected_content:
#            self.assertTrue(line.find(content) > -1)
