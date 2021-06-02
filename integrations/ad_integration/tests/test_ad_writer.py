# TODO: Fix imports in module
import copy
from unittest import mock
from unittest import TestCase

from jinja2.exceptions import UndefinedError
from more_itertools import first_true
from parameterized import parameterized

from ..ad_exceptions import CprNotFoundInADException
from ..ad_exceptions import CprNotNotUnique
from ..ad_writer import LoraCacheSource
from ..utils import AttrDict
from .test_utils import dict_modifier
from .test_utils import mo_modifier
from .test_utils import TestADWriterMixin
from exporters.utils.lazy_dict import LazyDict

JOB_TITLE_AD_FIELD_NAME = "titel"
JOB_TITLE_TEMPLATE = "{{ ad_values.get('titel') or mo_values['title'] }}"


class TestADWriter(TestCase, TestADWriterMixin):
    def setUp(self):
        self._setup_adwriter()

    def _verify_identitical_common_code(self, num_expected_scripts, num_common_lines=5):
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

    def _add_to_template_to_ad_fields(self, ad_field_name, template):
        return dict_modifier(
            {"integrations.ad_writer.template_to_ad_fields": {ad_field_name: template}}
        )

    def _assert_script_contains_field(self, script, name, value):
        """Assert that AD PowerShell script `script` includes the AD field
        called `name` and that the field value is `value`.
        """
        self.assertRegex(script, f'"{name}"="{value}"')

    @parameterized.expand(
        [
            # Test without any changes to settings
            [
                dict_modifier({}),
            ],
            # Test with overridden password
            [
                dict_modifier({"primary": {"password": "Password1"}}),
            ],
            [
                dict_modifier({"primary": {"password": "Hunter2"}}),
            ],
            # Test with overridden user
            [
                dict_modifier({"primary": {"system_user": "R2D2"}}),
            ],
            [
                dict_modifier({"primary": {"system_user": "C-3PO"}}),
            ],
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
            (
                "$UserCredential = New-Object –TypeName $TypeName"
                "–ArgumentList $User, $PWord"
            ),
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
            # Test mo_to_ad_fields
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.mo_to_ad_fields": {
                            "unit_user_key": "unit_user_key"
                        }
                    }
                ),
                None,
                '"unit_user_key"="Musik";',
            ],
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
        self._setup_adwriter(None, mo_transformer, settings_transformer)
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
            '-Name "'
            + mo_values["full_name"]
            + " - "
            + mo_values["sam_account_name"]
            + '"',
            '-Displayname "' + mo_values["full_name"] + '"',
            '-GivenName "' + mo_values["name"][0] + '"',
            '-SurName "' + mo_values["name"][1] + '"',
            '-SamAccountName "' + mo_values["sam_account_name"] + '"',
            '-EmployeeNumber "' + mo_values["employment_number"] + '"',
            "-Credential $usercredential",
            '-UserPrincipalName "' + mo_values["sam_account_name"] + '@epn_end"',
            "-OtherAttributes",
            '"level2orgunit_field"="Ingen";',
            '"org_field"="Kommune\\Forvalting\\Enhed\\";',
            '"uuid_field"="' + mo_values["uuid"] + '";',
            '"cpr_field"="'
            + mo_values["cpr"][0:6]
            + "ad_cpr_sep"
            + mo_values["cpr"][6:]
            + '"',
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
            "-Filter 'SamAccountName -eq \"" + mo_values["sam_account_name"] + "\"'",
            "-Credential $usercredential",
            "|",
            "Set-ADUser",
            "-Credential $usercredential",
            '-Displayname "' + mo_values["full_name"] + '"',
            '-GivenName "' + mo_values["name"][0] + '"',
            '-SurName "' + mo_values["name"][1] + '"',
            '-EmployeeNumber "' + mo_values["employment_number"] + '"',
            "-Replace",
            '"level2orgunit_field"="Ingen";',
            '"org_field"="Kommune\\Forvalting\\Enhed\\";',
        ]
        for content in expected_content:
            self.assertIn(content, edit_user_ps)

    def test_user_create_ad_values(self):
        """Test that `ad_values` is present (and empty) when creating a new AD
        user. The template `JOB_TITLE_TEMPLATE` uses the MO job title to fill
        in the job title in the appropriate AD field.
        """
        self._setup_adwriter(
            early_transform_settings=self._add_to_template_to_ad_fields(
                JOB_TITLE_AD_FIELD_NAME,
                JOB_TITLE_TEMPLATE,
            )
        )
        mo_uuid = "invalid-provided-and-accepted-due-to-mocking"
        self.ad_writer.create_user(mo_uuid, create_manager=False)
        # Find the "create AD user" script
        create_script = first_true(
            self.ad_writer.scripts, pred=lambda s: "New-ADUser" in s
        )
        # Assert that it contains the mapped job title field
        self._assert_script_contains_field(
            create_script, JOB_TITLE_AD_FIELD_NAME, self.mo_values_func()["title"]
        )

    @parameterized.expand(
        [
            # Tuples of (AD job title, which value to use)
            (None, "mo-value"),
            ("", "mo-value"),
            ("Tester", "ad-value"),
        ]
    )
    def test_user_update_ad_values(self, ad_value, expectation):
        """Test that `ad_values` is present (and empty) when updating an
        existing AD user. The template `JOB_TITLE_TEMPLATE` only populates the
        AD job title with the MO job title if the current AD job title is
        empty.
        """
        self._setup_adwriter(
            early_transform_settings=self._add_to_template_to_ad_fields(
                JOB_TITLE_AD_FIELD_NAME,
                JOB_TITLE_TEMPLATE,
            ),
            transform_ad_values=dict_modifier(
                {JOB_TITLE_AD_FIELD_NAME: ad_value},
            ),
        )
        mo_uuid = "invalid-provided-and-accepted-due-to-mocking"
        mo_value = self.mo_values_func()["title"]
        self.ad_writer.sync_user(mo_uuid, ad_dump=None, sync_manager=False)
        # Find the "update AD user" script
        update_script = first_true(
            self.ad_writer.scripts, pred=lambda s: "Set-ADUser" in s
        )
        # Assert that the update script uses the current AD job title
        self._assert_script_contains_field(
            update_script,
            JOB_TITLE_AD_FIELD_NAME,
            ad_value if ad_value else mo_value,
        )

    def test_duplicate_ad_field_entries(self):
        """Test user edit ps_script code.

        The common code is not tested.
        """
        # These keys conflict, and thus, no primary_write settings are emitted
        settings_transformer = dict_modifier(
            {
                "integrations.ad_writer.template_to_ad_fields": {
                    "Name": "{{ mo_values['unit'] }}"
                },
                "integrations.ad_writer.mo_to_ad_fields": {"unit": "name"},
            }
        )
        with self.assertRaises(ValueError):
            self._setup_adwriter(early_transform_settings=settings_transformer)

    def test_non_overwritten_default(self):
        """Test that a mistake in overriding a default results in an error."""
        # Assert no scripts were produced from initializing ad_writer itself
        self.assertGreaterEqual(len(self.ad_writer.scripts), 0)

        # Add another mapping for the display name AD field
        # In `TestADMixin._prepare_settings.default_settings`, this is spelled
        # `Displayname` (notice the case difference.)
        display_name = {"displayname": "{{ mo_values['name'][0] }}"}
        self.settings["primary_write"]["template_to_ad_fields"].update(display_name)

        with self.assertRaises(ValueError):
            self.ad_writer.sync_user(
                mo_uuid="invalid-provided-and-accepted-due-to-mocking",
                sync_manager=False,
            )

    def test_user_edit_illegal_parameter(self):
        """Test user edit ps_script code with illegal parameter

        The common code is not tested.

        This test simply ensures that the illegal parameter is dropped.
        """
        # Assert no scripts were produced from initializing ad_writer itself
        self.assertGreaterEqual(len(self.ad_writer.scripts), 0)
        import ad_template_engine

        ad_template_engine.illegal_parameters["Set-ADUser"].append("Displayname")

        settings_transformer = dict_modifier({})
        self._setup_adwriter(settings_transformer)
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
        self.assertNotIn("Displayname", edit_user_ps)

    def test_user_edit_illegal_attribute(self):
        """Test user edit ps_script code with illegal attribute.

        The common code is not tested.

        This test simply ensures that the illegal attribute is dropped.
        """
        # Assert no scripts were produced from initializing ad_writer itself
        self.assertGreaterEqual(len(self.ad_writer.scripts), 0)

        settings_transformer = dict_modifier(
            {"integrations.ad_writer.template_to_ad_fields": {"Name": "John"}}
        )

        self._setup_adwriter(settings_transformer)
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
        self.assertNotIn('"Name"="John"', edit_user_ps)

    def test_duplicated_ad_field_name(self):
        # Test configuration where 'Name' is mapped both via
        # `mo_to_ad_fields` *and* `template_to_ad_fields`
        add_dupe_field = dict_modifier(
            {
                "integrations.ad_writer.mo_to_ad_fields": {"unit": "Name"},
            }
        )
        with self.assertRaises(ValueError):
            self._setup_adwriter(early_transform_settings=add_dupe_field)

    @parameterized.expand(
        [
            # Verify different employment numbers
            [
                dict_modifier({}),
                mo_modifier({"employment_number": "267"}),
                dict_modifier({}),
                [],
            ],
            [
                dict_modifier({}),
                mo_modifier({"employment_number": "42"}),
                dict_modifier({}),
                [],
            ],
            # Test mo_to_ad_fields
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.mo_to_ad_fields": {"unit": "Name"},
                    }
                ),
                mo_modifier({"employment_number": "42"}),
                dict_modifier(
                    {
                        "name": ("John Deere", "Enhed"),
                    }
                ),
                ["Name"],
            ],
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.mo_to_ad_fields": {
                            "unit": "Name",
                            "employment_number": "extension_field2",
                        },
                    }
                ),
                mo_modifier({"employment_number": "42"}),
                dict_modifier(
                    {
                        "name": ("John Deere", "Enhed"),
                        "extension_field2": (None, "42"),
                    }
                ),
                ["Name"],
            ],
            # Test template_to_ad_fields
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "adjusted_number": (
                                "{{ mo_values['employment_number']|int + 5 }}",
                            )
                        },
                    }
                ),
                mo_modifier({"employment_number": "42"}),
                dict_modifier(
                    {
                        "adjusted_number": (None, "47"),
                    }
                ),
                [],
            ],
            # Test template_to_ad_fields
            [
                dict_modifier(
                    {
                        "integrations.ad_writer.template_to_ad_fields": {
                            "adjusted_number": (
                                "{{ mo_values['employment_number']|int + 5 }}",
                            ),
                            "Enabled": "Invalid",
                        },
                    }
                ),
                mo_modifier(
                    {
                        "employment_number": "42",
                    }
                ),
                dict_modifier(
                    {
                        "adjusted_number": (None, "47"),
                        "enabled": (True, "Invalid"),
                    }
                ),
                [],
            ],
        ]
    )
    def test_sync_compare(
        self,
        settings_transformer,
        mo_transformer,
        expected_transformer,
        removed_fields,
    ):
        # If test case defines `removed_fields`, remove these fields from the
        # `integrations.ad_writer.template_to_ad_fields` setting, as they are
        # defined by the `integrations.ad_writer.mo_to_ad_fields` setting
        # instead.
        def remove_template_fields(settings):
            key = "integrations.ad_writer.template_to_ad_fields"
            to_remove = set([f.lower() for f in removed_fields])
            for field in copy.copy(settings[key]):
                if field.lower() in to_remove:
                    del settings[key][field]
            return settings

        def actual_settings_transformer(settings):
            return settings_transformer(remove_template_fields(settings))

        self._setup_adwriter(
            early_transform_settings=actual_settings_transformer,
            transform_mo_values=mo_transformer,
        )

        uuid = "invalid-provided-and-accepted-due-to-mocking"
        mo_values = self.ad_writer.read_ad_information_from_mo(uuid)
        mo_values["manager_cpr"] = None
        ad_values = self._prepare_get_from_ad(lambda x: x)
        ad_values["Name"] = "John Deere"

        def find_ad_user(cpr, ad_dump=None):
            return ad_values

        self.ad_writer._find_ad_user = find_ad_user

        mismatch = self.ad_writer._sync_compare(mo_values, None)
        expected = {
            "level2orgunit_field": (None, "Ingen"),
            "org_field": (None, "Kommune\\Forvalting\\Enhed\\"),
            "name": (ad_values["Name"], mo_values["name_sam"]),
            "displayname": (None, " ".join(mo_values["name"])),
            "givenname": (ad_values["GivenName"], mo_values["name"][0]),
            "surname": (None, mo_values["name"][1]),
            "employeenumber": (None, mo_values["employment_number"]),
        }
        expected = expected_transformer(expected)
        self.assertEqual(mismatch, expected)

        # Apply changes to AD, and check _sync_compare returns nothing
        ad_keys = ad_values.keys()
        for lower_ad_key, changeset in expected.items():
            cased_ad_key = next(
                filter(lambda ad_key: ad_key.lower() == lower_ad_key, ad_keys),
                lower_ad_key,
            )
            ad_values[cased_ad_key] = changeset[1]
        mismatch = self.ad_writer._sync_compare(mo_values, None)
        self.assertEqual(mismatch, {})

    def test_sync_compare_context_includes_ad_values(self):
        self._setup_adwriter()

        # The `ad_values` passed in the template context has all keys in lower
        # case
        ad_values = {key.lower(): val for key, val in self.ad_values_func().items()}

        mo_values = self.ad_writer.read_ad_information_from_mo("mo-uuid")
        mo_values["manager_cpr"] = None

        # Call `_sync_compare` with a mocked `_render_field_template` method
        # so we can inspect the template contexts passed to it
        with mock.patch.object(self.ad_writer, "_render_field_template") as m:
            self.ad_writer._sync_compare(mo_values, None)

        # Assert that all calls to mocked template render method includes a
        # valid `ad_values` dict in the template context
        template_contexts = [
            call.args[0] for call in m.mock_calls if len(call.args) == 2
        ]
        self.assertTrue(
            all(context["ad_values"] == ad_values for context in template_contexts)
        )

    def test_add_manager(self):
        mo_values = self.ad_writer.read_ad_information_from_mo(
            uuid="0", read_manager=True
        )
        self.ad_writer.add_manager_to_user(
            "MGORE", manager_sam=mo_values["manager_sam"]
        )
        # Expected outputs
        num_expected_scripts = 1
        self.assertEqual(len(self.ad_writer.scripts), num_expected_scripts)
        # Verify that the first 4 lines are identitical for all scripts
        self._verify_identitical_common_code(num_expected_scripts)
        # Check that the create user ps looks good
        add_manager_ps = self.ad_writer.scripts[0].split("\n")[5].strip()

        expected_line = (
            "Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"'"
            + " -Credential $usercredential |Set-ADUser -Manager "
            + mo_values["manager_sam"]
            + " -Credential $usercredential"
        )
        self.assertEqual(add_manager_ps, expected_line)

    def test_set_password(self):
        password = "password"
        self.ad_writer.set_user_password("MGORE", password)
        # Expected outputs
        num_expected_scripts = 1
        self.assertEqual(len(self.ad_writer.scripts), num_expected_scripts)
        # Verify that the first 4 lines are identitical for all scripts
        self._verify_identitical_common_code(num_expected_scripts)
        # Check that the create user ps looks good
        set_password_ps = self.ad_writer.scripts[0].split("\n")[5].strip()

        expected_line = (
            "Get-ADUser -Filter 'SamAccountName -eq \"MGORE\"' -Credential"
            + " $usercredential |Set-ADAccountPassword -Reset -NewPassword"
            + ' (ConvertTo-SecureString -AsPlainText "{}" -Force)'
            + " -Credential $usercredential"
        ).format(password)
        self.assertEqual(set_password_ps, expected_line)

    @parameterized.expand(
        [
            # AD dump is None
            (None, CprNotFoundInADException),
            # AD dump is empty list
            ([], CprNotFoundInADException),
            # AD dump has user without CPR
            ([{"foo": "bar"}], CprNotFoundInADException),
            # AD dump has exactly one matching user
            ([{"cpr": "112233-4455"}], None),
            # AD dump has multiple users with identical CPRs
            (
                [{"cpr": "112233-4455"}, {"cpr": "112233-4455"}],
                CprNotNotUnique,
            ),
        ]
    )
    def test_find_ad_user_ad_dump(self, ad_dump, expected_exception):
        cpr = "112233-4455"

        def settings_transformer(settings):
            settings["integrations.ad"][0].update({"cpr_field": "cpr"})
            return settings

        self._setup_adwriter(
            early_transform_settings=settings_transformer, mock_find_ad_user=False
        )

        if expected_exception:
            with self.assertRaises(expected_exception):
                self.ad_writer._find_ad_user(cpr, ad_dump=ad_dump)
        else:
            ad_user = self.ad_writer._find_ad_user(cpr, ad_dump=ad_dump)
            self.assertDictEqual(ad_user, {"cpr": "112233-4455"})

    def test_template_fails_on_undefined_variable(self):
        settings_transformer = dict_modifier(
            {
                "integrations.ad_writer.template_to_ad_fields": {
                    "Displayname": "{{ unknown_variable }}"
                },
            }
        )
        self._setup_adwriter(early_transform_settings=settings_transformer)
        with self.assertRaises(UndefinedError):
            self.ad_writer.sync_user(mo_uuid="mo-uuid", sync_manager=False)

    def test_fullnames_are_empty_when_constituants_are_empty(self):
        self._setup_adwriter()
        uuid = "some_uuid_here"

        def get_mo_values(firstname, surname, nickname_firstname, nickname_surname):
            self.user = {
                "uuid": "some_uuid_here",
                "navn": "some_name some_lastname",
                "efternavn": surname,
                "fornavn": firstname,
                "kaldenavn": "",
                "kaldenavn_fornavn": nickname_firstname,
                "kaldenavn_efternavn": nickname_surname,
                "cpr": "some_cpr",
            }
            self.lc = AttrDict(
                {
                    "users": {
                        self.user["uuid"]: [self.user],
                    },
                    "engagements": {
                        "engagement_uuid": [
                            {
                                "user": self.user["uuid"],
                                "primary_boolean": True,
                                "user_key": "some_userkey",
                                "job_function": "job_function_title_uuid",
                                "unit": "some_unit",
                                "uuid": "engagement_uuid",
                            }
                        ]
                    },
                    "classes": {"job_function_title_uuid": {"title": "some_job_title"}},
                }
            )
            self.lc_historic = self.lc
            self.ad_writer.datasource = LoraCacheSource(self.lc, self.lc_historic, None)
            mo_values = self.ad_writer._read_ad_information_from_mo(uuid)
            return mo_values

        mo_values = get_mo_values("Ursula", "Uniknavn", "Anne", "Jensen")
        assert isinstance(mo_values, LazyDict)
        assert mo_values["name"] == ("Ursula", "Uniknavn")
        assert mo_values["full_name"] == "Ursula Uniknavn"

        assert mo_values["nickname"] == ("Anne", "Jensen")
        assert mo_values["full_nickname"] == "Anne Jensen"

        mo_values = get_mo_values("", "", "", "")
        assert isinstance(mo_values, LazyDict)
        assert mo_values["name"] == ("", "")
        assert mo_values["full_name"] == ""

        assert mo_values["nickname"] == ("", "")
        assert mo_values["full_nickname"] == ""
