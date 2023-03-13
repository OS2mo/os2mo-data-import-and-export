# sletter AD-brugere:
#   når det er mere end en måned siden at de blev deaktiveret,
#       ('extensionAttribute9' indeholder en dato for deaktivering, og denne dato er før 'idag minus en måned'.)
#   og som er inaktive,
#       ('Enabled' er 'false')
#   og som findes i eller under det relevante OU
#       ('-SearchBase' sættes til 'OU=Svendborg Kommune,DC=Svendborg,DC=net')

# Svendborg settings
#$DeactivationDateField = "extensionAttribute9"
#$SearchBase = "OU=Kapel,OU=OS2MO,DC=Svendborg,DC=net"

# Magenta internal AD settings
$DeactivationDateField = "info"
$SearchBase = "OU=Kapel,OU=demo,OU=OS2MO,DC=ad,DC=addev"

# Common settings
$EarliestDate = ((Get-Date).AddMonths(-1)).ToString('yyyy-MM-dd')
$SettingLogPath = "C:/temp/"
$SettingLogsToKeep = 30

function Start-Log {
    $Timestamp = Get-Date -Format "D.dd.MM.yyyy.T.HH.mm.ss"
    $LogFilePath = $SettingLogPath + "Log." + $Timestamp + ".txt"

    # Slet gamle log filer
    Get-ChildItem -Path $SettingLogPath | Sort-Object CreationTime -Descending | Select -Skip $SettingLogsToKeep | Remove-Item -Force

    Start-Transcript -Path $LogFilePath
}

function Delete-Inactive {
    Get-ADUser `
        -SearchBase $SearchBase `
        -Filter '$DeactivationDateField -le $EarliestDate -and Enabled -eq "false"' `
        -Properties $DeactivationDateField `
    | ForEach-Object {
        Write-Host "Påbegynder sletning af" $_ " (dato for deaktivering er" $_.$DeactivationDateField ")"
        Remove-ADUser $_ -Confirm:$false -Verbose
        Write-Host "Fuldførte sletning af" $_
    }
}

Start-Log
Delete-Inactive
Stop-Transcript
