from jinja2 import Template

from utils import dict_map, dict_partition, duplicates

# Parameters that should not be quoted
no_quote_list = ["Credential"]


cmdlet_parameters = {
    # https://docs.microsoft.com/en-us/powershell/module/activedirectory/new-aduser
    "New-ADUser": {
        "AccountExpirationDate",
        "AccountNotDelegated",
        "AccountPassword",
        "AllowReversiblePasswordEncryption",
        "AuthType",
        "CannotChangePassword",
        "Certificates",
        "ChangePasswordAtLogon",
        "City",
        "Company",
        "CompoundIdentitySupported",
        "Country",
        "Credential",
        "Department",
        "Description",
        "DisplayName",
        "Division",
        "EmailAddress",
        "EmployeeID",
        "EmployeeNumber",
        "Enabled",
        "Fax",
        "GivenName",
        "HomeDirectory",
        "HomeDrive",
        "HomePage",
        "HomePhone",
        "Initials",
        "Instance",
        "KerberosEncryptionType",
        "LogonWorkstations",
        "Manager",
        "MobilePhone",
        "Name",
        "Office",
        "OfficePhone",
        "Organization",
        "OtherAttributes",
        "OtherName",
        "PasswordNeverExpires",
        "PasswordNotRequired",
        "Path",
        "POBox",
        "PostalCode",
        "PrincipalsAllowedToDelegateToAccount",
        "ProfilePath",
        "SamAccountName",
        "ScriptPath",
        "Server",
        "ServicePrincipalNames",
        "SmartcardLogonRequired",
        "State",
        "StreetAddress",
        "Surname",
        "Title",
        "TrustedForDelegation",
        "Type",
        "UserPrincipalName",
        # "WhatIf", "Confirm", "PassThru",
    },
    # https://docs.microsoft.com/en-us/powershell/module/activedirectory/set-aduser
    "Set-ADUser": {
        "AccountExpirationDate",
        "AccountNotDelegated",
        "Add",
        "AllowReversiblePasswordEncryption",
        "AuthType",
        "CannotChangePassword",
        "Certificates",
        "ChangePasswordAtLogon",
        "City",
        "Clear",
        "Company",
        "CompoundIdentitySupported",
        "Country",
        "Credential",
        "Department",
        "Description",
        "DisplayName",
        "Division",
        "EmailAddress",
        "EmployeeID",
        "EmployeeNumber",
        "Enabled",
        "Fax",
        "GivenName",
        "HomeDirectory",
        "HomeDrive",
        "HomePage",
        "HomePhone",
        "Identity",
        "Initials",
        "KerberosEncryptionType",
        "LogonWorkstations",
        "Manager",
        "MobilePhone",
        "Office",
        "OfficePhone",
        "Organization",
        "OtherName",
        "Partition",
        "PasswordNeverExpires",
        "PasswordNotRequired",
        "POBox",
        "PostalCode",
        "PrincipalsAllowedToDelegateToAccount",
        "ProfilePath",
        "Remove",
        "Replace",
        "SamAccountName",
        "ScriptPath",
        "Server",
        "ServicePrincipalNames",
        "SmartcardLogonRequired",
        "State",
        "StreetAddress",
        "Surname",
        "Title",
        "TrustedForDelegation",
        "UserPrincipalName",
        # "WhatIf", "Confirm", "PassThru",
    },
}

# These may never be emitted in other_attributes
illegal_attributes = ["Credential", "Name"]


cmdlet_templates = {
    "New-ADUser": """
        New-ADUser
        {%- for parameter, value in parameters.items() %}
          -{{ parameter }} {{ value }}
        {%- endfor %}
          -OtherAttributes @{
        {%- for attribute, value in other_attributes.items() -%}
            "{{ attribute }}"={{ value }};
        {%- endfor -%}
        }
    """,
    # Update information saved on a user
    # Notice: Name cannot be updated using Set-ADUser, this must be done
    # with Rename-AdObject
    # TODO: Consider Replace versus Remove/Clean/Add
    "Set-ADUser": """
        Get-ADUser
          -Filter 'SamAccountName -eq {{ parameters['SamAccountName'] }}'
          -Credential {{ parameters['Credential'] }} |
        Set-ADUser
        {%- for parameter, value in parameters.items() %}
          -{{ parameter }} {{ value }}
        {%- endfor %}
          -Replace @{
        {%- for attribute, value in other_attributes.items() -%}
            "{{ attribute }}"={{ value }};
        {%- endfor -%}
        }
    """,
}


def lower_list(listy):
    """Convert each element in the list to lower-case.

    Example:
        result = lower_list(['Alfa', 'BETA', 'gamma'])
        self.assertEqual(result, ['alfa', 'beta', 'gamma'])

    Args:
        listy: The list of strings to force into lowercase.

    Returns:
        list: A list where all contained the strings are lowercase.
    """
    return list(map(lambda x: x.lower(), listy))


def prepare_default_field_templates(jinja_map):
    """Expand jinja_map with default templates.

    Args:
        jinja_map: dictionary from ad field names to jinja template strings.

    Returns:
        dict: A jinja_map which has been extended with default templates.
    """
    # Seed default templates
    jinja_map.setdefault(
        "Name",
        "{{ mo_values['name'][0] }} {{ mo_values['name'][1] }} - {{ user_sam }}",
    )
    jinja_map.setdefault(
        "Displayname", "{{ mo_values['name'][0] }} {{ mo_values['name'][1] }}"
    )
    jinja_map.setdefault("GivenName", "{{ mo_values['name'][0] }}")
    jinja_map.setdefault("SurName", "{{ mo_values['name'][1] }}")
    jinja_map.setdefault("EmployeeNumber", "{{ mo_values['employment_number'] }}")
    return jinja_map


def prepare_settings_based_field_templates(jinja_map, cmd, settings):
    """Expand jinja_map with settings based templates.

    Args:
        jinja_map: dictionary from ad field names to jinja template strings.
        cmd: command to generate template for.
        settings: dictionary containing settings from settings.json

    Returns:
        dict: A jinja_map which has been extended with settings based values.
    """
    # Build settings-based templates
    def _get_setting_type(settings, key):
        # TODO: Currently we ignore school
        try:
            return settings[key]
        except KeyError:
            msg = "Unable to find settings type: " + key
            raise Exception(msg)

    write_settings = _get_setting_type(settings, "primary_write")
    primary_settings = _get_setting_type(settings, "primary")
    jinja_map[
        write_settings["level2orgunit_field"]
    ] = "{{ mo_values['level2orgunit'] }}"
    jinja_map[write_settings["org_field"]] = "{{ mo_values['location'] }}"

    # Local fields for MO->AD sync'ing
    named_sync_fields = write_settings.get("mo_to_ad_fields")
    for mo_field, ad_field in named_sync_fields.items():
        jinja_map[ad_field] = "{{ mo_values[" + mo_field + "] }}"

    # Local fields for MO->AD sync'ing
    named_sync_template_fields = write_settings.get("template_to_ad_fields")
    for ad_field, template in named_sync_template_fields.items():
        jinja_map[ad_field] = template

    if cmd == "New-ADUser":  # New user
        jinja_map["UserPrincipalName"] = (
            "{{ user_sam }}@" + write_settings["upn_end"]
        )
        jinja_map[write_settings["uuid_field"]] = "{{ mo_values['uuid'] }}"

        # If local settings dictates a separator, we add it directly to the
        # power-shell code.
        jinja_map[write_settings["cpr_field"]] = (
            "{{ mo_values['cpr'][0:6] }}"
            + primary_settings["cpr_separator"]
            + "{{ mo_values['cpr'][6:10] }}"
        )

    return jinja_map


def prepare_and_check_login_field_templates(jinja_map):
    """Check validity and expand jinja_map with login templates.

    Args:
        jinja_map: dictionary from ad field names to jinja template strings.

    Returns:
        dict: A jinja_map which has been extended with login templates.
    """
    # Check against hardcoded values, as these will be forcefully overridden.
    jinja_keys = lower_list(jinja_map.keys())
    if "Credential".lower() in jinja_keys:
        raise ValueError("Credential is hardcoded")
    if "SamAccountName".lower() in jinja_keys:
        raise ValueError("SamAccountName is hardcoded")
    # Do the forceful override
    jinja_map["Credential"] = "$usercredential"
    jinja_map["SamAccountName"] = "{{ user_sam }}"

    return jinja_map


def prepare_template(cmd, jinja_map, settings):
    """Build a complete powershell command template.

    Args:
        cmd: command to generate template for.
        jinja_map: dictionary from ad field names to jinja template strings.
        settings: dictionary containing settings from settings.json

    Returns:
        str: A jinja template string produced by templating the command
             template with all the field templates.
    """
    # Load command template via cmd
    # cmd = 'Set-ADUser'
    cmd_options = cmdlet_templates.keys()
    if cmd not in cmd_options:
        raise ValueError(
            "prepare_template cmd must be one of: " + ",".join(cmd_options)
        )
    command_template = Template(cmdlet_templates[cmd])

    # Load field templates (ad_field --> template)
    jinja_map = prepare_default_field_templates(jinja_map)
    jinja_map = prepare_settings_based_field_templates(jinja_map, cmd, settings)
    jinja_map = prepare_and_check_login_field_templates(jinja_map)

    # Check against duplicates in jinja_map
    ad_fields_low = map(lambda ad_field: ad_field.lower(), jinja_map.keys())
    duplicate_ad_fields = duplicates(ad_fields_low)
    if duplicate_ad_fields:
        raise ValueError("Duplicate ad_field: " + ",".join(duplicate_ad_fields))

    # Put quotes around all values outside the no_quote_list
    def quotes_wrap(value, key):
        if key.lower() in lower_list(no_quote_list):
            return value
        return '"{}"'.format(value)

    jinja_map = dict_map(quotes_wrap, jinja_map)

    # Partition rendered attributes by parameters and attributes
    parameter_list = lower_list(cmdlet_parameters[cmd])
    other_attributes, parameters = dict_partition(
        lambda key, _: key.lower() in parameter_list, jinja_map
    )

    # Drop all illegal attributes
    for attribute in illegal_attributes:
        other_attributes.pop(attribute, None)

    # Generate our combined template, by rendering our command template using
    # the jinja_map templates.
    combined_template = command_template.render(
        parameters=parameters, other_attributes=other_attributes
    )
    return combined_template


def template_powershell(context, settings, cmd="New-ADUser", jinja_map=None):
    """Build a complete powershell command.

    Args:
        cmd: command to generate template for. Defaults to 'New-ADUser'.
        jinja_map: dictionary from ad field names to jinja template strings.
        context: dictionary used for jinja templating context.
        settings: dictionary containing settings from settings.json

    Returns:
        str: An executable powershell script.
    """
    # Set arguments to empty dicts if none
    jinja_map = jinja_map or {}

    # Acquire the full template, templated itself with all field templates
    full_template = prepare_template(cmd, jinja_map, settings)

    # Render the final template using the context
    return Template(full_template).render(**context)
