from jinja2 import Template
from utils import dict_partition

cmdlet_parameters = {
    # https://docs.microsoft.com/en-us/powershell/module/activedirectory/new-aduser
    'New-ADUser': {
       "AccountExpirationDate", "AccountNotDelegated", "AccountPassword", "AllowReversiblePasswordEncryption", "AuthType", "CannotChangePassword", "Certificates", "ChangePasswordAtLogon", "City", "Company", "CompoundIdentitySupported", "Country", "Credential", "Department", "Description", "DisplayName", "Division", "EmailAddress", "EmployeeID", "EmployeeNumber", "Enabled", "Fax", "GivenName", "HomeDirectory", "HomeDrive", "HomePage", "HomePhone", "Initials", "Instance", "KerberosEncryptionType", "LogonWorkstations", "Manager", "MobilePhone", "Name", "Office", "OfficePhone", "Organization", "OtherAttributes", "OtherName", "PasswordNeverExpires", "PasswordNotRequired", "Path", "POBox", "PostalCode", "PrincipalsAllowedToDelegateToAccount", "ProfilePath", "SamAccountName", "ScriptPath", "Server", "ServicePrincipalNames", "SmartcardLogonRequired", "State", "StreetAddress", "Surname", "Title", "TrustedForDelegation", "Type", "UserPrincipalName",
       # "WhatIf", "Confirm", "PassThru",
    },
    # https://docs.microsoft.com/en-us/powershell/module/activedirectory/set-aduser
    'Set-ADUser': {
       "AccountExpirationDate", "AccountNotDelegated", "Add", "AllowReversiblePasswordEncryption", "AuthType", "CannotChangePassword", "Certificates", "ChangePasswordAtLogon", "City", "Clear", "Company", "CompoundIdentitySupported", "Country", "Credential", "Department", "Description", "DisplayName", "Division", "EmailAddress", "EmployeeID", "EmployeeNumber", "Enabled", "Fax", "GivenName", "HomeDirectory", "HomeDrive", "HomePage", "HomePhone", "Identity", "Initials", "KerberosEncryptionType", "LogonWorkstations", "Manager", "MobilePhone", "Office", "OfficePhone", "Organization", "OtherName", "Partition", "PasswordNeverExpires", "PasswordNotRequired", "POBox", "PostalCode", "PrincipalsAllowedToDelegateToAccount", "ProfilePath", "Remove", "Replace", "SamAccountName", "ScriptPath", "Server", "ServicePrincipalNames", "SmartcardLogonRequired", "State", "StreetAddress", "Surname", "Title", "TrustedForDelegation", "UserPrincipalName",
       # "WhatIf", "Confirm", "PassThru",
    }
}

cmdlet_templates = {
    'New-ADUser': """
        New-ADUser
        {%- for parameter, value in parameters.items() %}
          -{{ parameter }} "{{ value }}"
        {%- endfor %}
          -OtherAttributes @{
        {%- for attribute, value in other_attributes.items() -%}
            "{{ attribute }}"="{{ value }}";
        {%- endfor -%}
        }
    """,
    # TODO: Consider Replace versus Remove/Clean/Add
    'Set-ADUser': """
        Get-ADUser
          -Filter 'SamAccountName -eq "{{ parameters['SamAccountName'] }}"'
          -Credential "{{ parameters['Credential'] }}" |
        Set-ADUser
        {%- for parameter, value in parameters.items() %}
          -{{ parameter }} "{{ value }}"
        {%- endfor %}
          -Replace @{
        {%- for attribute, value in other_attributes.items() -%}
            "{{ attribute }}"="{{ value }}";
        {%- endfor -%}
        }
    """,
}


def lower_list(listy):
    """Convert each element in the list to lower-case."""
    return list(map(lambda x: x.lower(), listy))


def prepare_template(cmd, jinja_map, settings):
    # Seed defaults
    jinja_map.setdefault('Name', "{{ mo_values['name'][0] }} {{ mo_values['name'][1] }} - {{ user_sam }}")
    jinja_map.setdefault('Displayname', "{{ mo_values['name'][0] }} {{ mo_values['name'][1] }}")
    jinja_map.setdefault('GivenName', "{{ mo_values['name'][0] }}")
    jinja_map.setdefault('SurName', "{{ mo_values['name'][1] }}")
    jinja_map.setdefault('EmployeeNumber', "{{ mo_values['employment_number'] }}")

    def _get_write_setting(settings):
        # TODO: Currently we ignore school
        if not settings['primary_write']:
            msg = 'Trying to enable write access with broken settings.'
            logger.error(msg)
            raise Exception(msg)
        return settings['primary_write']

    write_settings = _get_write_setting(settings)
    jinja_map[write_settings['level2orgunit_field']] = "{{ mo_values['level2orgunit'] }}"
    jinja_map[write_settings['org_field']] = "{{ mo_values['location'] }}"

    # Local fields for MO->AD sync'ing
    named_sync_fields = settings.get('integrations.ad_writer.mo_to_ad_fields', {})
    for mo_field, ad_field in named_sync_fields.items():
        jinja_map[ad_field] = '{{ mo_values[' + mo_field + '] }}'

    if cmd == 'New-ADUser': # New user
        jinja_map['UserPrincipalName'] = "{{ user_sam }}@" + write_settings['upn_end']
        jinja_map[write_settings['uuid_field']] = "{{ mo_values['uuid'] }}"

        # If local settings dictates a separator, we add it directly to the
        # power-shell code.
        jinja_map[write_settings['cpr_field']] = "{{ mo_values['cpr'][0:6] }}" + settings['integrations.ad.cpr_separator'] + "{{ mo_values['cpr'][6:10] }}"

    # Check against hardcoded values
    jinja_keys = lower_list(jinja_map.keys())
    if 'Credential'.lower() in jinja_keys:
        raise ValueError("Credential is hardcoded")
    if 'SamAccountName'.lower() in jinja_keys:
        raise ValueError("SamAccountName is hardcoded")
    jinja_map['Credential'] = "$usercredential"
    jinja_map['SamAccountName'] = "{{ user_sam }}"

    return jinja_map


def template_create_user(cmd='New-ADUser', jinja_map=None, context=None, settings=None):
    # Set arguments to empty dicts if none
    jinja_map = jinja_map or {}
    context = context or {}
    settings = settings or {}

    # Add SAM to mo_values
    # context['mo_values']['name_sam'] = '{} - {}'.format(context['mo_values']['full_name'], context['user_sam'])

    # Load command template via cmd
    #cmd = 'Set-ADUser'
    command_template = Template(cmdlet_templates[cmd])

    # Load field templates via cmd
    # ad_field --> template
    jinja_map = prepare_template(cmd, jinja_map, settings)

    # Partition rendered attributes by parameters and attributes
    parameter_list = lower_list(cmdlet_parameters[cmd])
    other_attributes, parameters = dict_partition(
        lambda key, _: key.lower() in parameter_list,
        jinja_map
    )

    # Generate our combined template, by rendering our command template using
    # the jinja_map templates.
    combined_template = command_template.render(
        parameters=parameters, other_attributes=other_attributes
    )

    # Render the final template using the context
    return Template(combined_template).render(**context)
