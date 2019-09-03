****************************
Active Directory Integration
****************************

Indledning
==========
Denne integration gør det muligt at læse informaition fra en lokal AD installation
med henblik på at anvende disse informationer ved import til MO.

Opsætning
=========

For at kunne afvikle integrationen kræves en række opsætninger af den lokale server.

Integrationen går via i alt tre maskiner:
 1. Den lokale server, som afvikler integrationen (typisk MO serveren selv).

 2. En remote management server, som den lokale server kan kommunikere med via
    Windows Remote Management (WinRM). Denne kommunikation autentificeres via
    Kerberos. Der findes en vejledning til opsætning af denne kommunikation her
    (LINK).

 3. AD serveren.

Når integratioen er i drift, genererer den PowerShell kommandoer som sendes til
remote management serveren som afvikler dem mod AD serveren. Denne omvej hænger
sammen med, at MO afvikles fra et Linux miljø, hvorimod PowerShell kommunikation
med AD bedst afvikles fra et Windows miljø. 

For at kunne afvikle integrationen kræves der udover den nævnte opsæting af Keberos,
at et antal miljøvariable er sat i det miljø integrationen køres fra:

Standard AD
-----------
 * ``WINRM_HOST``: Hostname på remote mangagent server
 * ``AD_SEARCH_BASE``
 * ``AD_CPR_FIELD``
 * ``AD_SYSTEM_USER``
 * ``AD_PROPERTIES``

Skole  AD
---------
