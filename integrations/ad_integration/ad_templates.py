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
