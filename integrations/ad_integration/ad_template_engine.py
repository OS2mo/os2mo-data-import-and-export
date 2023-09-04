from typing import Any
from typing import Dict

from jinja2 import Environment
from jinja2 import StrictUndefined
from jinja2 import Template

from .utils import dict_map
from .utils import dict_partition
from .utils import duplicates
from .utils import lower_list


class InvalidValue:
    _value = "<invalid value>"

    def __str__(self):
        return self._value

    def __eq__(self, other):
        return self._value == other


INVALID = InvalidValue()


# Parameters that should not be quoted
no_quote_list = ["Credential", "Enabled", "AccountPassword"]


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

# These may never be emitted in parameters / other_attributes
illegal_parameters = {
    "New-ADUser": ["Manager"],
    "Set-ADUser": ["Manager", "Name"],
}
illegal_attributes = {
    "New-ADUser": ["Credential", "Name"],
    "Set-ADUser": ["Credential", "Name"],
}

cmdlet_templates = {
    "New-ADUser": """
        New-ADUser
        {%- for parameter, value in parameters.items() %}
          -{{ parameter }} {{ value }}
        {%- endfor %}
        {% if other_attributes %}
          -OtherAttributes @{
        {%- for attribute, value in other_attributes.items() -%}
            "{{ attribute }}"={{ value }};
        {%- endfor -%}
        }
        {% endif %}
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
        {% if other_attributes %}
          -Replace @{
        {%- for attribute, value in other_attributes.items() -%}
            "{{ attribute }}"={{ value }};
        {%- endfor -%}
        }
        {% endif %}
    """,
}


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
            result = settings[key]
        except KeyError:
            msg = "Unable to find settings type: " + key
            print(msg)
        if not result:
            raise Exception("%r is empty" % key)
        return result

    write_settings = _get_setting_type(settings, "primary_write")
    primary_settings = _get_setting_type(settings, "primary")

    if write_settings.get("level2orgunit_field"):
        jinja_map[
            write_settings["level2orgunit_field"]
        ] = "{{ mo_values['level2orgunit'] }}"

    jinja_map[write_settings["org_field"]] = "{{ mo_values['location'] }}"

    # Local fields for MO->AD sync'ing
    named_sync_fields = write_settings.get("mo_to_ad_fields")
    for mo_field, ad_field in named_sync_fields.items():
        jinja_map[ad_field] = "{{ mo_values['" + mo_field + "'] }}"

    # Local fields for MO->AD sync'ing
    named_sync_template_fields = write_settings.get("template_to_ad_fields")
    for ad_field, template in named_sync_template_fields.items():
        jinja_map[ad_field] = template

    if cmd == "New-ADUser":  # New user
        jinja_map["UserPrincipalName"] = "{{ user_sam }}@" + write_settings["upn_end"]
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
    if "credential" in jinja_keys:
        raise ValueError("Credential is hardcoded")
    if "samaccountname" in jinja_keys:
        raise ValueError("SamAccountName is hardcoded")
    if "manager" in jinja_keys:
        raise ValueError("Manager is handled uniquely")
    # Do the forceful override
    jinja_map["Credential"] = "$usercredential"
    jinja_map["SamAccountName"] = "{{ user_sam }}"

    return jinja_map


def prepare_field_templates(cmd, settings, jinja_map=None):
    """Build a finalized map of parameters and attributes.

    Args:
        cmd: command to generate template for.
        settings: dictionary containing settings from settings.json
        jinja_map: dictionary from ad field names to jinja template strings.

    Returns:
        tuple(dict, dict):
            parameters: a dict of parameter key, value pairs
            other_attributes: a dict of attribute key, value pairs

            Both dicts have the same format, namely:
                field_name -> jinja template for the field
    """
    # Load field templates (ad_field --> template)
    jinja_map = jinja_map or {}
    jinja_map = prepare_settings_based_field_templates(jinja_map, cmd, settings)
    jinja_map = prepare_and_check_login_field_templates(jinja_map)

    # Check against duplicates in jinja_map
    ad_fields_low = map(lambda ad_field: ad_field.lower(), jinja_map.keys())
    duplicate_ad_fields = duplicates(ad_fields_low)
    if duplicate_ad_fields:
        raise ValueError("Duplicate ad_field: " + ",".join(duplicate_ad_fields))
    return jinja_map


def quote_templates(jinja_map):
    # Put quotes around all values outside the no_quote_list
    def quotes_wrap(value, key):
        if key.lower() in lower_list(no_quote_list):
            return value
        return '"{}"'.format(value)

    jinja_map = dict_map(jinja_map, value_func=quotes_wrap)
    return jinja_map


def partition_templates(cmd, jinja_map):
    # Partition rendered attributes by parameters and attributes
    parameter_list = lower_list(cmdlet_parameters[cmd])
    other_attributes, parameters = dict_partition(
        lambda key, _: key.lower() in parameter_list, jinja_map
    )
    return parameters, other_attributes


def filter_illegal(cmd, parameters, other_attributes):

    # Drop all illegal parameters and attributes
    # Parameters
    for parameter in illegal_parameters[cmd]:
        parameters.pop(parameter, None)
        parameters.pop(parameter.lower(), None)
    # Attributes
    for attribute in illegal_attributes[cmd]:
        other_attributes.pop(attribute, None)
        other_attributes.pop(attribute.lower(), None)

    return parameters, other_attributes


def filter_empty_values(
    environment: Environment,
    attrs: Dict[str, str],
    context: Dict[str, Any],
) -> Dict[str, str]:
    """Remove key/template pairs from `attrs` if the template renders the value
    "\"None\"".
    """
    to_remove = set()

    for name, template_code in attrs.items():
        template = load_jinja_template(environment, template_code)
        value = template.render(**context)
        if value in ('"None"', "", f'"{INVALID}"'):
            to_remove.add(name)

    for attribute_name in to_remove:
        del attrs[attribute_name]

    return attrs


def load_jinja_template(environment: Environment, source: str) -> Template:
    """Load Jinja template in the string `source` and return a `Template`
    instance.
    """
    return environment.from_string(source)


def prepare_template(environment: Environment, cmd, settings, context):
    """Build a complete powershell command template.

    Args:
        environment: Jinja2 `Environment` instance
        cmd: command to generate template for.
        settings: dictionary containing settings from settings.json

    Returns:
        str: A jinja template string produced by templating the command
             template with all the field templates.
    """
    # Load command template via cmd
    cmd_options = cmdlet_templates.keys()
    if cmd not in cmd_options:
        raise ValueError(
            "prepare_template cmd must be one of: " + ",".join(cmd_options)
        )

    command_template = load_jinja_template(environment, cmdlet_templates[cmd])

    parameters, other_attributes = filter_illegal(
        cmd,
        *partition_templates(
            cmd, quote_templates(prepare_field_templates(cmd, settings))
        ),
    )

    parameters = filter_empty_values(environment, parameters, context)
    other_attributes = filter_empty_values(environment, other_attributes, context)

    # Generate our combined template, by rendering our command template using
    # the field templates templates.
    combined_template = command_template.render(
        parameters=parameters, other_attributes=other_attributes
    )
    return combined_template


def template_powershell(
    context,
    settings,
    cmd: str = "New-ADUser",
    environment: Environment = Environment(undefined=StrictUndefined),
) -> str:
    """Build a complete powershell command.

    Args:
        context: dictionary used for jinja templating context.
        settings: dictionary containing settings from settings.json
        cmd: command to generate template for. Defaults to 'New-ADUser'.
        environment: Jinja template environment

    Returns:
        str: An executable powershell script.
    """
    # Acquire the full template, templated itself with all field templates
    full_template = prepare_template(environment, cmd, settings, context)

    # Render the final template using the context
    final_template = load_jinja_template(environment, full_template)
    return final_template.render(**context)


def render_update_by_mo_uuid_cmd(
    complete: str,
    credentials: str,
    uuid_field: str,
    uuid_value: str,
    field_map: dict[str, str],
    environment: Environment = Environment(undefined=StrictUndefined),
) -> str:
    """Build a 'Set-ADUser' Powershell command using the MO UUID field in AD.

    Args:
        complete: must be `AD._ps_boiler_plate()["complete"]`.
        credentials: must be `AD._ps_boiler_plate()["credentials"]`.
        uuid_field: name of the AD field containing the MO user UUID.
        uuid_value: MO user UUID (as string.)
        field_map: dictionary mapping AD field names to their new values.
        environment: Jinja template environment.

    Returns:
        str: An executable powershell script.
    """

    # Quote field names in `field_map`, if required by Powershell
    field_map_quoted: dict = quote_templates(field_map)

    # Divide `field_map` into "parameters" and "other_attributes"
    parameters: dict
    other_attributes: dict
    parameters, other_attributes = partition_templates("Set-ADUser", field_map_quoted)

    context: dict = {
        "complete": complete,
        "credentials": credentials,
        "uuid_field": uuid_field,
        "uuid_value": uuid_value,
        "parameters": parameters,
        "other_attributes": other_attributes,
    }

    cmd_template: str = """
        Get-ADUser {{ complete }} -Filter '{{ uuid_field }} -eq "{{ uuid_value }}"' |
        Set-ADUser {{ credentials }}
        {% for parameter, value in parameters.items() %}
            -{{ parameter }} {{ value }}
        {% endfor %}
        {% if other_attributes %}
            -Replace @{
            {%- for attribute, value in other_attributes.items() -%}
                "{{ attribute }}"={{ value }}{%- if not loop.last -%};{%- endif -%}
            {%- endfor -%}
            }
        {% endif %} |
        ConvertTo-Json
    """
    cmd_template_jinja: Template = load_jinja_template(environment, cmd_template)
    return cmd_template_jinja.render(context)
