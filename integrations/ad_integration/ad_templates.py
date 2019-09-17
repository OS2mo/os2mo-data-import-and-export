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
 -Name "{givenname} {surname} - {sam_account_name}"
 -Displayname "{givenname} {surname}"
 -GivenName "{givenname}"
 -SurName "{givenname} {surname}"
 -SamAccountName "{sam_account_name}"
 -EmployeeNumber "{employment_number}"
 -Credential $usercredential
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



# Update informtion saved on a user
# Notice: Name cannot be updated using Set-ADUser, this must be done
# with Rename-AdObject
edit_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{sam_account_name}\"'
            -Credential $usercredential |
Set-ADUser -Credential $usercredential
 -Displayname "{givenname} {surname}"
 -GivenName "{givenname}"
 -SurName "{givenname} {surname}"
 -EmployeeNumber "{employment_number}"
"""
