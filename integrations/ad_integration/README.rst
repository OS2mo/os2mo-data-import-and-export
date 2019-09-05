.. _Integration til Active Directory:

********************************
Integration til Active Directory
********************************

Indledning
==========
Denne integration gør det muligt at læse information fra en lokal AD installation med
henblik på at anvende disse informationer ved import til MO.

Opsætning
=========

For at kunne afvikle integrationen kræves en række opsætninger af den lokale server.

Integrationen går via i alt tre maskiner:

 1. Den lokale server, som afvikler integrationen (typisk MO serveren selv).

 2. En remote management server som den lokale server kan kommunikere med via
    Windows Remote Management (WinRM). Denne kommunikation autentificeres via
    Kerberos. Der findes en vejledning til opsætning:
    https://os2mo.readthedocs.io/en/latest/_static/AD%20-%20OS2MO%20ops%C3%A6tnings%20guide.pdf

 3. AD serveren.

Når integrationen er i drift, genererer den PowerShell kommandoer som sendes til
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
 * ``AD_PROPERTIES``: Liste over felter som skal læses fra AD. Angives som en streng
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
om der er oprettet de nødvendige Kerberos tokens og miljøvariable.


Brug af integrationen
=====================

Integrationen anvendes ved at slå brugere op via cpr nummer. Det er muligt at slå op
på enten et specifikt cpr-nummer, på en søgning med wild card, eller man kan lave
et opslag på alle brugere, som derved caches i integrationen hvorefter opsalg på
enkelte cpr-numre vil ske næsten instantant. Den indledende cache skabes i praksis
ved at itererere over alle cpr-numre ved hjælp af kald til 01*, 02* etc.

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


Skrivning til AD
================

Der udvikles i øjeblikket en udvidesle til AD integrationen som skal muliggøre at
oprette AD brugere og skrive information fra MO til relevante felter.

Hvis denne funktionalitet skal benyttes, er der brug for yderligere parametre som
skal være sat når programmet afvikles:

 * ``AD_WRITE_UUID``: Navnet på det felt i AD, hvor MOs bruger-uuid skrives.
 * ``AD_WRITE_UNIT``: Navnet på det felt i AD, hvor MO skriver navnet på den enhed
   hvor medarbejderen har sin primære ansættelse.
 * ``AD_WRITE_ORG``: Navnet på det felt i AD, hvor MO skriver enhedshierakiet for
   den enhed, hvor medarbejderen har sin primære ansættelse.


Skabelse af brugernavne
-----------------------

For at kunne oprette brugere i AD, er det nødvendigt at kunne tildele et
SamAccountName til de nye brugere. Til dette formål findes i modulet ``user_names``
klassen ``CreateUserNames``. Programmet startes ved at instantiere klassen med en
liste over allerede reserverede eller forbudte navne som parametre, og det er
herefter muligt at forespørge AD om en liste over alle brugenavne som er i brug, og
herefter er programet klar til at lave brugernavne.

.. code-block:: python

    from user_names import CreateUserName
    
    name_creator = CreateUserNames(occupied_names=set())
    name_creator.populate_occupied_names()

    name = ['Karina', 'Munk', 'Jensen']
    print(name_creator.create_username(name))
    
    name = ['Anders', 'Kristian', 'Jens', 'Peter', 'Andersen']
    print(name_creator.create_username(name))

    
Brugernavne konstrureres efter en forholdsvis specifik algoritme som fremgår af
koden.
