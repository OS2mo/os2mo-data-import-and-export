# sletter AD-brugere:
#   når det er mere end en måned siden at de blev deaktiveret,
#       ('extensionAttribute9' indeholder en dato for deaktivering, og denne dato er før 'idag minus en måned'.)
#   og som er inaktive,
#       ('Enabled' er 'false')
#   og som findes i eller under det relevante OU
#       ('-SearchBase' sættes til 'OU=Svendborg Kommune,DC=Svendborg,DC=net')
$SearchBase = "OU=Kapel,OU=OS2MO,DC=Svendborg,DC=net"
$EarliestDate = ((Get-Date).AddMonths(-1)).ToString('yyyy-MM-dd')

Get-ADUser `
-SearchBase $SearchBase `
-Filter 'extensionAttribute9 -le $EarliestDate -and Enabled -eq "false"' `
-Properties * `
| Remove-ADUser -Confirm:$false -Verbose
