# Add manager to user
add_manager_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{user_sam}\"' -Credential $usercredential |
Set-ADUser -Manager {manager_sam} -Credential $usercredential
"""


rename_user_template = """
Get-ADUser -Filter 'SamAccountName -eq \"{user_sam}\"' -Credential $usercredential |
Rename-ADobject -Credential $usercredential -NewName "{new_name}"
"""
