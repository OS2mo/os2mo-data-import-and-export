# TODO:
# All templates might need to also inlclude bp['server'],
# no relevant test-case is currently available.
# Set password for account
set_password_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{username}\"'
           -Credential $usercredential |

Set-ADAccountPassword
   -Reset
   -NewPassword (ConvertTo-SecureString
                 -AsPlainText "{password}" -Force)
    -Credential $usercredential
"""


# Enable AD account
enable_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{username}\"'
            -Credential $usercredential |
Enable-ADAccount -Credential $usercredential
"""

# Disable AD account
disable_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{username}\"'
            -Credential $usercredential |
Disable-ADAccount -Credential $usercredential
"""


# Delete user
delete_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{username}\"' -Credential $usercredential |
Remove-ADUser -Credential $usercredential -Confirm:$false
"""


# Add manager to user
add_manager_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{user_sam}\"' -Credential $usercredential |
Set-ADUser -Manager {manager_sam} -Credential $usercredential
"""


rename_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{user_sam}\"' -Credential $usercredential |
Rename-ADobject -Credential $usercredential -NewName "{new_name}"
"""
