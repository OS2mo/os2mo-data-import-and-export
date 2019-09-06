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

# Create user
create_user_template = """
New-ADUser
-Name "{} - {}"
-Displayname "{}"
-GivenName "{}"
-SurName "{}"
-SamAccountName "{}"
-EmployeeNumber "{}"
"""


# Delete user
delete_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{username}\"' -Credential $usercredential |
Remove-ADUser -Credential $usercredential -Confirm:$false
"""
