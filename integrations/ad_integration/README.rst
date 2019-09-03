********************************
Integration til Active Directory
********************************

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

For at kunne afvikle integrationen kræves der udover den nævnte opsætning af Keberos,
at AD er sat op med cpr-numre på medarbejdere samt en servicebruger som har
rettigheder til at læse dette felt. Desuden skal et antal miljøvariable være sat i
det miljø integrationen køres fra:

Fælles parametre
----------------

 * ``WINRM_HOST``: Hostname på remote mangagent server

Standard AD
-----------

 * ``AD_SEARCH_BASE``: Search base, eksempelvis 'OU=enheder,DC=kommune,DC=local'
 * ``AD_CPR_FIELD``: Navnet på feltet i AD som indeholder cpr nummer.
 * ``AD_SYSTEM_USER``: Navnet på den systembruger som har rettighed til at læse fra
   AD.
 * ``AD_PASSWORD``: Password til samme systembruger.
 * ``AD_PROPERTIES``: Liste over feler som skal læses fra AD, angives som en streng
   med mellemrum, eks: "xAttrCPR ObjectGuid SamAccountName Title EmailAddress
   MobilePhone"

Skole  AD
---------

Hvis der ønskes integration til et AD til skoleområdet, udover det almindelige
administrative AD, skal disse parametre desuden angives. Hvis de ikke er til stede
ved afviklingen, vil integrationen ikke forsøge at tilgå et skole AD.

 * ``AD_SCHOOL_SEARCH_BASE``
 * ``AD_SCHOOL_CPR_FIELD``
 * ``AD_SCHOOL_SYSTEM_USER``
 * ``AD_SCHOOLE_PASSWORD``
 * ``AD_SCHOOL_PROPERTIES``

Test af opsætningen
-------------------

Der følger med AD integrationen et lille program, ``test_connectivity.py`` som tester
om der oprettet de nødvendige Kerberos tokens og miljøvariable.


Brug af integrationen
=====================

Integrationen anvendes ved at slå brugere op via cpr nummer. Det er muligt at slå
på enten et specifikt cpr-nummer, på en søgning med wild card, eller man kan lave
et opslag på alle brugere, som derved caches i integrationen hvorefter opsalg på
enkelte cpr-numre herefter vil skre næsten instantant. Den indledende cache skabes i
praksis ved at itererere over alle cpr-numre ved hjælp af kald til 01*, 02* etc.

Ved anvendelse af både administrativt AD og skole AD vil brugere først blive slået op
i skole AD og dernæst i administrativt AD, hvis medarbejderen findes begge steder vil
det således blive elementet fra det administrative AD som vil ende med at blive
returneret.

.. code-block:: python

   import ad_reader

   ad_reader = ad_reader.ADParameterReader()

   # Læs alle medarbejdere ind fra AD.
   ad_reader.cache_all()

   # De enkelte opslag går nu direkte til cache og returnerer med det samme
   user = ad_reader.read_user(cpr=cpr, cache_only=True)

Objektet ``user`` vil nu indeholde de felter der er angivet i miljøvariablen
``AD_PROPERTIES``.
