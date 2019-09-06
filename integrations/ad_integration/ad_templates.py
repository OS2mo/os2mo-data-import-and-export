# TODO:
# This template might need to also inlclude bp['server'],
# no relevant test-case is currently available.
set_password_template = """

Get-ADUser -Filter 'SamAccountName -eq \"{username}\"' {credentials} |

Set-ADAccountPassword
   -Reset
   -NewPassword (ConvertTo-SecureString
                 -AsPlainText "{password}" -Force)
    {credentials}
"""


# TODO:
# This template might need to also inlclude bp['server'],
# no relevant test-case is currently available.
enable_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{username}\"' {credentials} |
Enable-ADAccount {credentials}
"""


create_user_template = """
New-ADUser
-Name "{} - {}"
-Displayname "{}"
-GivenName "{}"
-SurName "{}"
-SamAccountName "{}"
-EmployeeNumber "{}"
"""
