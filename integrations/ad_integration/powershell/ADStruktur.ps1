# mrw, 21.2.2023:
#
# * Justeret med flg. ændringer:
#
# * Rensning af enhedsnavne omskriver kun komma til "" (ingenting.)
#
# * Adskillelse mellem "hvor findes de u-placerede AD-brugere?", og "under hvilken OU skal de placeres under?"
#   Hhv. $SettingOURoot og $SettingOURootCreate
#
# * Skip en eller flere navngivne OU'er i "CleanUpEmptyOU" ("Computere", "Brugere".)
# * Kald til "CleanUpEmptyOU" er udkommenteret som default.
#
# * Skip et eller flere led i den organisatoriske sti, når brugere skal indplaceres.
#   Eksempel:
#       Brugerens organisatoriske sti er "Top\Led 2\Led 3\Led 4"
#       $SettingOURootCreate er "OU=Kommune,DC=dc,DC=com"
#       $SettingStartAtDepth er 1
#   Brugeren indplaceres i "OU=Led 4,OU=Led 3,OU=Led 2,OU=Kommune,DC=dc,DC=com"
#   "Top" i den organisatoriske sti springes over, fordi $SettingStartAtDepth er 1 (= skip første led)
#
#$ScriptVersion = 2.8

# ----- Script parameter ----- #
Param (
    [String]$Username # Kør script for én bruger, ved at angive det som parameter ved kørsel af script.
)

# Svendborg settings
$SettingOURoot = "OU=Nye Brugere,OU=OS2MO,DC=Svendborg,DC=net" # Skal være oprettet. Angiver roden for hvor scriptet skal arbejde fra.
$SettingOURootCreate = "OU=Test-OU,OU=OS2MO,DC=Svendborg,DC=net"
$SettingOUInactiveUsers = "OU=Kapel,OU=OS2MO,DC=Svendborg,DC=net" # Skal være oprettet. Angiver OU for inaktive brugere.
$SettingAutoritativOrg = "extensionAttribute10" # Angiv navnet på den attribut som indeholder den autoritative organisation
$SettingStartAtDepth = 0

# ----- Script settings ---------- #
#$SettingOURoot = "OU=Users,OU=demo,OU=OS2MO,DC=ad,DC=addev" # Skal være oprettet, og indeholder AD-brugere oprettet af MO
#$SettingOURootCreate = "OU=OS2MO,DC=ad,DC=addev"  # Rod for de OU'er, som dette script opretter
#$SettingOUInactiveUsers = "OU=Kapel,OU=demo,OU=OS2MO,DC=ad,DC=addev" # Skal være oprettet. Angiver OU for inaktive brugere
#$SettingAutoritativOrg = "Description" # Angiv navnet på den attribut som indeholder den autoritative organisation
#$SettingStartAtDepth = 1  # Angiver hvor mange led i den organisatoriske sti, der skal springes over ved indplacering
$SettingLogPath = "C:/temp/" # Angiv stien til hvor log filer skal placeres (Format: "C:\PSScript\")
$SettingLogsToKeep = 30 # Angiv hvor mange log filer der skal gemmes (der laves en ny log fil med dato og tid, for hver kørsel af scriptet)

# ----- Bruger settings: Angiv hvilke brugere scripet skal køre for (hvis der ikke er angivet $Username som parameter ved kørsel) ----- #
If (!$Username){
    # Kør for alle brugere, i forhold til $SettingOURoot
    $SettingUsers = Get-ADUser -Filter * -SearchBase $SettingOURoot -Properties $SettingAutoritativOrg
}

#region Script
# ----- Faste variabler --- #
[int]$global:movedcount = 0
[int]$global:EmptyOUsRemoved = 0

# ----- Funktioner -------- #
# Renser organisationsnavnet for uønskede tegn
function Get-SanitizedUTF8Input{
    Param(
        [String]$inputString
    )
    $replaceTable = @{","=""}
    foreach($key in $replaceTable.Keys){$inputString = $inputString -Replace($key,$replaceTable.$key)}
    return $inputString
}

# Placer bruger i OU struktur ud fra den autoritive organisation. Opretter stuktur hvis den ikke findes.
function Set-AutoritativOrg {
    Param (
        [Parameter(Mandatory=$true)]$Username,
        [string]$OURoot = $SettingOURootCreate, # Default value, kan overskrives under kørsel af funktion
        [string]$AutoritativOrg = $SettingAutoritativOrg # Default value, kan overskrives under kørsel af funktion
    )

    # Henter AD information omkring den aktuelle bruger
    $User = $Username

    # Undersøger om Autoriativorg er sat, ellers springer vi denne bruger over.
    If ($user.$AutoritativOrg -eq $null) {
        Write-host "Skipping: Attributten $AutoritativOrg er tom" -ForegroundColor Red
        Return
    }

    # Undersøger om brugeren er inaktiv og placeret under inaktive brugere
    If (($Username.DistinguishedName.Split(",",2)[1] -eq $SettingOUInactiveUsers) -and ($Username.Enabled -eq $false)){
        Write-host "Skipping: Brugeren er inaktiv:"$username.samaccountname"" -ForegroundColor Green
        Return
    }

    # Flytter inaktive brugere til OU for inaktive brugere (alle deaktiverede brugere i roden)
    If (($Username.DistinguishedName.Split(",",2)[1] -eq $SettingOURoot) -and ($Username.Enabled -eq $false)){
        Write-host "Brugeren er inaktiv og flyttes til: $SettingOUInactiveUsers" -ForegroundColor Green
        Move-ADObject -Identity $Username.objectguid -TargetPath $SettingOUInactiveUsers -Verbose
        $global:movedcount++
        Return
    }

    ## Vi arbejder nu med en aktiveret bruger, som evt. skal flyttes ##
    $AutoritativOrgDisplayName = $Username.$AutoritativOrg

    # Renser organisationnavnet for uønskede tegn
    $AutoritativOrg = get-sanitizedUTF8Input -inputString $Username.$AutoritativOrg

    # Omskriv til OU path
    $StartAtDepth = $SettingStartAtDepth
    $AutoritativOrgOU = ($AutoritativOrg.Split("\")[$StartAtDepth..100]) | foreach {$_ = "OU="+$_;$_}
    [array]::Reverse($AutoritativOrgOU)
    $AutoritativOrgOU = $AutoritativOrgOU -join ","

    # Displayname
    $AutoritativOrgOUDisplayName = $AutoritativOrgDisplayName.Split("\") | foreach {$_ = "OU="+$_;$_}
    [array]::Reverse($AutoritativOrgOUDisplayName)
    $AutoritativOrgOUDisplayName = $AutoritativOrgOUDisplayName -join ","

    # Sammensæt den fulde OU sti
    $path = $AutoritativOrgOU+","+$OURoot

    # Tjek om brugeren allerede er placeret rigtigt
    If ((($Username.DistinguishedName.Split(",")[1..100]) -join ",") -eq $path){
         Write-host "Brugeren er allerede placeret det rigtige sted: $path" -ForegroundColor Green
         Return
    }

    # Hvis mappen findes, så placer brugeren her
    If ([adsi]::Exists("LDAP://$path") -eq $true) {
        Write-host "OU findes, brugeren flyttes til: $path" -ForegroundColor Green
        Move-ADObject -Identity $Username.objectguid -TargetPath $path -Verbose
        $global:movedcount++
    }

    # Hvis mappen ikke findes, så test hver del af stien og opret det der mangler.
    Else {
        $niveauer = $AutoritativOrgOU -split(",")
        $niveauerDisplayName = $AutoritativOrgOUDisplayName -split(",")
        $count = -2

        #Tjek om første led findes, ellers opret den
        $currentpath = ($niveauer[-1] -join ",")+","+$OURoot
        If ([adsi]::Exists("LDAP://$currentpath")){
            Write-host "Første led findes" $currentpath -ForegroundColor Green
        }
        Else {
            write-host "Opretter første led" -ForegroundColor Yellow
            $Name = (($niveauer[-1]).replace("OU=",""))
            $NameDisplayName = (($niveauerDisplayName[-1]).replace("OU=",""))
            New-ADOrganizationalUnit -name $name -DisplayName $NameDisplayName -Path $OURoot -ProtectedFromAccidentalDeletion $false -Verbose
        }

        # Tjek om alle underliggende led findes, ellers opret dem
        Do {
            $currentpath = ($niveauer[$count..-1] -join ",")+","+$OURoot

            If ([adsi]::Exists("LDAP://$currentpath")){
                Write-host "Findes" $currentpath -ForegroundColor Green
            }
            Else {
                write-host "Opretter underliggende sti" -ForegroundColor Yellow
                $newpath = (($niveauer[($count+1)..-1] -join ",")+","+$OURoot)
                $Name = (($niveauer[$count]).replace("OU=",""))
                $NameDisplayName = (($niveauerDisplayName[$count]).replace("OU=",""))
                New-ADOrganizationalUnit -name $name -DisplayName $NameDisplayName -Path $newpath -ProtectedFromAccidentalDeletion $false -Verbose
            }
        $count--
        } Until ($count -eq ((-$niveauer.count)-1))

        # Placer brugeren i brugerens OU
        Write-host "Brugeren places her: $currentpath" -ForegroundColor Green
        Move-ADObject -Identity $Username.objectguid -TargetPath $path -Verbose
    }
}

# Ryder op i OU strukturen, ved at fjerne tomme OU'er
function CleanUpEmptyOU {
Do{
    # Flere navne kan tilføjes nedenfor (Computere, Brugere, osv.)
    $EmptyOU = Get-ADOrganizationalUnit -Filter * -SearchBase $SettingOURootCreate `
      | Where-Object DistinguishedName -NotMatch "OU=Computere," `
      | Where-Object DistinguishedName -NotMatch "OU=Brugere," `
      | ForEach-Object { If (!(Get-ADObject -Filter * -SearchBase $_ -SearchScope OneLevel)) {$_}}

    ForEach ($OU in $EmptyOU) {
        # Vi sletter ikke OU'en til inaktive brugere, selvom den evt. skulle være tom.
        If ($OU.DistinguishedName -ne $SettingOUInactiveUsers){
            Set-ADOrganizationalUnit -Identity $OU.DistinguishedName -ProtectedFromAccidentalDeletion $false
            Remove-ADOrganizationalUnit -Identity $OU.DistinguishedName -Confirm:$false
            Write-host "Følgende OU er tom og dermed fjernet: "$OU.DistinguishedName -ForegroundColor Green
            $global:EmptyOUsRemoved++
        }
    }
} While ($EmptyOU.Count -ge 1)

}

# Funktion til logning
function Start-Log {
$Timestamp = Get-Date -Format "D.dd.MM.yyyy.T.HH.mm.ss"

# Angiver unikt navn til log filen
$LogFilePath = $SettingLogPath + "Log." + $Timestamp + ".txt"

# Slet gamle log filer
Get-ChildItem -Path $SettingLogPath | Sort-Object CreationTime -Descending | Select -Skip $SettingLogsToKeep | Remove-Item -Force

Start-Transcript -Path $LogFilePath
}

# ----- Run ----- #

# Starter transcript logning
Start-Log

# Kør for én bestemt bruger, hvis det er angivet som parameter ved kørsel af script.
If($Username){
    Try{$SettingUsers = Get-ADUser -Identity $Username -Properties $SettingAutoritativOrg}Catch{Write-host "Ugyldig bruger angivet som parameter" -ForegroundColor Red;Stop-Transcript;Return}
}

$time = Measure-Command {

    # Foretag flytning af brugere og oprettelse af OU'er
    Foreach ($User in $SettingUsers) {
        #Write-host "Starter process for"$User.samaccountname -ForegroundColor Yellow
        Set-AutoritativOrg -Username $user
        #Write-host "Færdig" -ForegroundColor Yellow
    }

    # Foretag oprydning i tomme OU'er
    #CleanUpEmptyOU

} | Select-Object TotalSeconds

Write-host "Total users: "$SettingUsers.count
Write-host "Moved users: "$global:movedcount
Write-host "Empty OUs removed:" $global:EmptyOUsRemoved
Write-host "Total time (sec):" $time.TotalSeconds
Write-host ""

# Stopper logning
Stop-Transcript
#endregion
