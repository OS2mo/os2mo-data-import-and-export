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

 * ``AD_SERVERS``: Liste med de DC'ere som findes i kommunens AD. Denne liste anvendes
   til at sikre at replikering er færdiggjort før der skrives til en nyoprettet
   bruger.
 * ``AD_WRITE_UUID``: Navnet på det felt i AD, hvor MOs bruger-uuid skrives.
 * ``AD_WRITE_FORVALTNING``: Navnet på det felt i AD, hvor MO skriver navnet på
   den forvaltning hvor medarbejderen har sin primære ansættelse.
 * ``AD_WRITE_ORG``: Navnet på det felt i AD, hvor MO skriver enhedshierakiet for
   den enhed, hvor medarbejderen har sin primære ansættelse.
 * ``PRIMARY_ENGAGEMENT_TYPE``: uuid på den ansættelsesklasse som markerer en
   primær ansættelse. Denne parameter vil i løbet af udvilingen blive generaliseret
   til en sorteret liste over forskellige engagementstyper som kan anses som
   primære.
 * ``FORVALTNING_TYPE``: uuid på den enhedstype som angiver at enheden er på
   forvaltingsnieau og derfor skal skrives i feltet angivet i
   ``AD_WRITE_FORVALTNING``.


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

    name = ['Olê', 'Østergård', 'Høst', 'Ærøe']
    print(name_creator.create_username(name))

Brugernavne konstrureres efter en forholdsvis specifik algoritme som fremgår af
koden.


Synkronisering
--------------

Der eksisterer (udvikles) to synkroniseringstjenester, en til at synkronisere felter
fra AD til MO, og en til at synkronisere felter fra MO til AD.

Synkronisering fra AD til MO foregår via programmet ``ad_sync.py``. Programmet vil
(for nuværende) i udgangspunktet opdaterere alle relevante værdier i MO fra de
tilsvarende i AD for alle medarbejdere.
Dette foregår ved at programmet først udtrækker samtlige medarbejdere fra MO, der
itereres hen over denne liste, og information fra AD'et slås op med cpr nummer som
nøgle. Hvis brugeren findes i AD, udlæses alle parametre angivet i ``AD_PROPERTIES``
og de relevante af dem synkroniseres til MO. Hvad der er relevant, angives i
øjeblikket som en hårdkodet liste direkte i synkroniseringsværktøkjet, de nuværende
eksempler går alle på forskellige former for adresser.

Da AD ikke understøtter gyldighedstider, antages alle informationer uddraget fra AD
at gælde fra 'i dag' og til evig tid.

Synkronisering fra MO til AD foregår efter en algoritme hvor der itereres hen over
alle AD brugere. Hver enkelt bruger slås op i MO via feltet `AD_WRITE_UUID` og
informatione fra MO synkroniseres til AD.


Opsætning for lokal brug af integrationen
=========================================

Flere af værktøjerne i AD integrationen er udstyret med et kommandolinjeinterface,
som kan anvendes til lokale tests. For at anvende dette er skal tre ting være på
plads i det lokale miljø:

 1. En lokal bruger med passende opsætning af kerberos til at kunne tilgå remote
    management serveren.
 2. De nødvendige miljøvariable med settings.
 3. Et lokalt pythonmiljø med passende afhængigheder

Angående punkt 1 skal dette opsættes af den lokale IT organisation, hvis man
har fulgt denne dokumentation så langt som til dette punkt, er der en god
sandsynlighed for at man befinder sig i et miljø, hvor dette allerede er på plads.

Punkt 2 gøres til lokale tests lettest ved at oprette en shell fil, som opretter de
nøvdendige miljøvarible.

::

   export AD_SYSTEM_USER=
   export AD_PASSWORD=
   export AD_SERVERS=
   export AD_SEARCH_BASE=

   export AD_WRITE_UUID=
   export AD_WRITE_FORVALTNING=
   export AD_WRITE_ORG=
   export AD_CPR_FIELD=
   export AD_PROPERTIES=

   export AD_SCHOOL_SYSTEM_USER=""
   export AD_SCHOOL_PASSWORD=""
   export AD_SCHOOL_PROPERTIES=""

   export WINRM_HOST=

   export MORA_BASE=http://localhost:5000
   export MOX_BASE=http://localhost:8080
   export SAML_TOKEN=

   export VISIBLE_CLASS=''
   export SECRET_CLASS=''
   export PRIMARY_ENGAGEMENT_TYPE=
   export FORVALTNING_TYPE=

Hvor betydniningen af de enkelte felter er angviet højere oppe i dokumentationen.
Felter som omhandler skolemdomænet er med vilje sat til blanke, da ingen af
skriveintegrationerne på dette tidspunkter undestøtter dette.

Når felterne er udfyldt kan den effektexures med kommandoen:

::
   source <filnavn>

Det skal nu oprettes et lokalt afviklingsmiljø. Dette gøres ved at klone git
projektet i en lokal mappe og oprette et lokal python miljø:

::
   git clone https://github.com/OS2mo/os2mo-data-import-and-export
   cd os2mo-data-import-and-export
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install os2mo_data_import
   pip install pywinrm[kerberos]

Der findes desværre i den nuærende udgave af `pywinrm` en fejl som gør det nødvendigt
at lave en rettelse direkte i en lokal installeret fil.

::
   nano venv/lib/python3.5/site-packages/winrm/__init__.py

Ret linjen:

::
   rs.std_err = self._clean_error_msg(rs.std_err)

Til:

::
   rs.std_err = self._clean_error_msg(rs.std_err.decode('utf-8'))


For at bekræfte at alt er på plads, findes et værktøj til at teste kommunikationen:

::
   cd integrations/ad_integration
   python test_connectivity.py

Hvis dette returnerer med ordet 'success' er integrationen klar til brug.
