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
rettigheder til at læse dette felt. Desuden skal et antal variable være sat i
``settings.json``

Fælles parametre
----------------

 * ``integrations.ad.winrm_host``: Hostname på remote mangagent server

Standard AD
-----------

 * ``integrations.ad.search_base``: Search base, eksempelvis
   'OU=enheder,DC=kommune,DC=local'
 * ``integrations.ad.cpr_field``: Navnet på feltet i AD som indeholder cpr nummer.
 * ``integrations.ad.system_user``: Navnet på den systembruger som har rettighed til
   at læse fra AD.
 * ``integrations.ad.password``: Password til samme systembruger.
 * ``integrations.ad.properties``: Liste over felter som skal læses fra AD. Angives
   som en liste i json-filen.


Skole  AD
---------

Hvis der ønskes integration til et AD til skoleområdet, udover det almindelige
administrative AD, skal disse parametre desuden angives som miljøvariable. Hvis de
ikke er til stede ved afviklingen, vil integrationen ikke forsøge at tilgå et
skole AD.

 * ``AD_SCHOOL_SEARCH_BASE``
 * ``AD_SCHOOL_CPR_FIELD``
 * ``AD_SCHOOL_SYSTEM_USER``
 * ``AD_SCHOOLE_PASSWORD``
 * ``AD_SCHOOL_PROPERTIES``

Test af opsætningen
-------------------

Der følger med AD integrationen et lille program, ``test_connectivity.py`` som tester
om der er oprettet de nødvendige Kerberos tokens og miljøvariable. Programmet
afvikles med en af to parametre:

 * ``--test-read-settings``
 * ``--test-write-settings``

En test af læsning foregår i flere trin:
 * Der testes for om Remote Managent serveren kan nås og autentificeres med et
   kereros token.
 * Der testes om det er muligt af afvikle en triviel kommando på AD serveren.
 * Der testes for, at en søgning på alle cpr-numre fra 31. november returnerer
   nul resultater.
 * Der testes for, at en søging på cpr-numre fra den 30. i alle måneder returner
   mindst et resultat. Hvis der ikke returneres nogen er fejlen efter sandsynligt
   en manglende rettighed til at læse cpr-nummer feltet.
 * Der tests om de returnerede svar indeholder mest et eksempel på disse tegn:
   æ, ø, å, @ som en test af et tegnsættet er korrekt sat op.
   
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

Objektet ``user`` vil nu indeholde de felter der er angivet i ``settings.json``
med nøglen ``integrations.ad.properties``.


Skrivning til AD
================

Der udvikles i øjeblikket en udvidesle til AD integrationen som skal muliggøre at
oprette AD brugere og skrive information fra MO til relevante felter.

Hvis denne funktionalitet skal benyttes, er der brug for yderligere parametre som
skal være sat når programmet afvikles:

 * ``integrations.ad.write.servers``: Liste med de DC'ere som findes i kommunens AD.
   Denne liste anvendes til at sikre at replikering er færdiggjort før der skrives
   til en nyoprettet bruger.
 * ``integrations.ad.write.uuid_field``: Navnet på det felt i AD, hvor MOs
   bruger-uuid skrives.
 * ``integrations.ad.write.forvaltning_field``: Navnet på det felt i AD, hvor MO
   skriver navnet på den forvaltning hvor medarbejderen har sin primære ansættelse.
 * ``integrations.ad.write.org_unit_field``: Navnet på det felt i AD, hvor MO
   skriver enhedshierakiet for den enhed, hvor medarbejderen har sin primære
   ansættelse.
 * ``integrations.ad.write.primary_types``: Sorteret lister over uuid'er på de
   ansættelsestyper som markerer en primær ansættelse. Jo tidligere et engagement
   står i listen, jo mere primært anses det for at være.
 * ``integrations.ad.write.forvaltning_type``: uuid på den enhedstype som angiver at
   enheden er på forvaltingsnieau og derfor skal skrives i feltet angivet i
   ``integrations.ad.write.forvaltning_field``.


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


Afvikling af PoerShell templates
---------------------------------

Det er muligt at angive PowerShell kode hvor visse værdier angives med abstrakte
refrencer til MO, som så på runtime vil bive udfyldt med de tilhørende værdier
for det person det drejer sig om.

for øjeblikket understøttes disse variable:

 * ``%OS2MO_AD_BRUGERNAVN%``
 * ``%OS2MO_BRUGER_FORNAVN%``
 * ``%OS2MO_BRUGER_EFTERNAVN%``
 * ``%OS2MO_BRUGER_CPR%``
 * ``%OS2MO_LEDER_EMAIL%``
 * ``%OS2MO_LEDER_NAVN%``
 * ``%OS2MO_BRUGER_ENHED%``
 * ``%OS2MO_BRUGER_ENHED_UUID%``

Hvis et script indeholder andre nøgler på formen %OS2MO_ ... % vil der returneres
en fejlmeddelelse (exception hvis det afvikles som en integration), med mindre
disse variale er udkommenteret.

Integrationen forventer at scripts befinder sig i mappen `scripts` i en undermappe
til integrationen selv, og alle scripts skal have en `ps_template` som filendelse.
Den tekniske platform for afvikling af scripts er den samme som for den øvrige AD
integration; scriptet sendes til remote management serveren, som afvikler scriptet.
Bemærk at scripts i denne kategori ikke nødvendigvis behøver have direkte kontakt
med AD, men vil kunne anvends til alle formål hvor der er behov for at afvikle
PowerShell med værdier fra MO.


Opsætning for lokal brug af integrationen
-----------------------------------------

Flere af værktøjerne i AD integrationen er udstyret med et kommandolinjeinterface,
som kan anvendes til lokale tests. For at anvende dette er skal tre ting være på
plads i det lokale miljø:

 1. En lokal bruger med passende opsætning af kerberos til at kunne tilgå remote
    management serveren.
 2. Den nødvendige konfiguration skal angives i ``settings.json``.
 3. Et lokalt pythonmiljø med passende afhængigheder

Angående punkt 1 skal dette opsættes af den lokale IT organisation, hvis man
har fulgt denne dokumentation så langt som til dette punkt, er der en god
sandsynlighed for at man befinder sig i et miljø, hvor dette allerede er på plads.

Punkt 2 gøres ved at oprette filen ``settings.json`` under mappen ``settings`` Et
anonymieret eksempel på sådan en fil kunne se sådan ud:

.. code-block:: json

   {
       "mox.base": "http://localhost:8080",
       "mora.base": "http://localhost:5000",
       "municipality.name": "Kommune Kommune",
       "municipality.code": 999,
       "integrations.SD_Lon.import.too_deep": ["Afdelings-niveau"],
       "integrations.SD_Lon.global_from_date": "2019-10-31",
       "integrations.SD_Lon.sd_user": "SDUSER",
       "integrations.SD_Lon.sd_password": "SDPASSWORD",
       "integrations.SD_Lon.institution_identifier": "AA",
       "integrations.SD_Lon.import.run_db": "/home/mo/os2mo-data-import-and-export/settings/change_at_runs.db",
       "address.visibility.secret": "53e9bbec-dd7b-42bd-b7ee-acfbaf8ac28a",
       "address.visibility.internal": "3fe99cdd-4ab3-4bd1-97ad-2cfb757f3cac",
       "address.visibility.public": "c5ddc7d6-1cd2-46b0-96de-5bfd88db8d9b",
       "integrations.ad.winrm_host": "rm_mangement_hostname",
       "integrations.ad.search_base": "OU=KK,DC=kommune,DC=dk",
       "integrations.ad.system_user": "serviceuser",
       "integrations.ad.password": "sericeuser_password",
       "integrations.ad.cpr_field": "ad_cpr_field",
       "integrations.ad.write.servers": [
	   "DC1",
	   "DC2",
	   "DC3",
	   "DC4",
	   "DC5"
       ],
       "integrations.ad.write.forvaltning_type": "cdd1305d-ee6a-45ec-9652-44b2b720395f",
       "integrations.ad.write.primary_types": [
	   "62e175e9-9173-4885-994b-9815a712bf42",
	   "829ad880-c0b7-4f9e-8ef7-c682fb356077",
	   "35c5804e-a9f8-496e-aa1d-4433cc38eb02"
       ],
       "integrations.ad.write.uuid_field": "sts_field",
       "integrations.ad.write.forvaltning_field": "extensionAttribute1",
       "integrations.ad.write.org_unit_field": "extensionAttribute2",
       "integrations.ad.properties": [
	   "manager",
	   "ObjectGuid",
	   "SamAccountName",
	   "mail",
	   "mobile",
	   "pager",
	   "givenName",
	   "l",
	   "sn",
	   "st",
	   "cn",
	   "company",
	   "title",
	   "postalCode",
	   "streetAddress",
	   "telephoneNumber",
	   "physicalDeliveryOfficeName",
	   "extensionAttribute1",
	   "extensionAttribute2",
	   "extensionAttribute3",
	   "extensionAttribute4",
	   "extensionAttribute5",
	   "extensionAttribute6",
	   "extensionAttribute7",
	   "extensionAttribute9",
	   "ad_cpr_field"
       ],
       "integrations.ad.ad_mo_sync_mapping": {
	   "user_addresses": {
	       "telephoneNumber": ["51d4dbaa-cb59-4db0-b9b8-031001ae107d", "PUBLIC"],
	       "pager": ["956712cd-5cde-4acc-ad0a-7d97c08a95ee", "SECRET"],
	       "mail": ["c8a49f1b-fb39-4ce3-bdd0-b3b907262db3", null],
	       "physicalDeliveryOfficeName": ["7ca6dfb1-5cc7-428c-b15f-a27056b90ae5", null],
	       "mobile": ["43153f5d-e2d3-439f-b608-1afbae91ddf6", "PUBLIC"]
	   },
	   "it_systems": {
	       "samAccountName": "fb2ac325-a1c4-4632-a254-3a7e2184eea7"
	   }
       }
   }


Hvor betydniningen af de enkelte felter er angviet højere oppe i dokumentationen.
Felter som omhandler skolemdomænet er foreløbig sat via miljøvariable og er ikke
inkluderet her, da ingen af skriveintegrationerne på dette tidspunkter undestøtter
dette.

Når felterne er udfyldt kan indstillingerne effektures med kommandoen:

Det skal nu oprettes et lokalt afviklingsmiljø. Dette gøres ved at klone git
projektet i en lokal mappe og oprette et lokal python miljø:

::

   git clone https://github.com/OS2mo/os2mo-data-import-and-export
   cd os2mo-data-import-and-export
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install os2mo_data_import/
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


Anvendelse af kommondolinjeværktøjer
------------------------------------

Følgende funktionaliteter har deres eget kommandolinjeværktøj som gør det muligt at
anvende dem uden at rette direkte i Python koden:

 * ``ad_writer.py``
 * ``execute_ad_script.py``
 * ``user_names.py``

For user names kræves der dog en del forudsætninger som gør at kommandolinjeværktøjet
ikke praksis har brugbar funktionalitet endnu.

ad_writer.py
++++++++++++

Dette værktøj har følgende muligheder:

::

   usage: ad_writer.py [-h]
                    [--create-user-with-manager MO_uuid |
		    --create-user MO_uuid |
		    --sync-user MO_uuid | --delete-user User_SAM |
		    --read-ad-information User_SAM |
		    --add-manager-to-user Manager_SAM User_SAM]

De forskellige muligheder gennemgås her en ad gangen:
 * --create-user-with-manager MO uuid

   Eksempel: python ad_writer-py --create-user-with-manager 4931ddb6-5084-45d6-9fb2-52ff33998005

   Denne kommando vil oprette en ny AD bruger ved hjælp af de informationer der er
   findes om brugeren i MO. De relevante felter i AD vil blive udfyld i henhold til
   den lokale feltmapning, og der vil blive oprettet et link til AD kontoen for
   lederen af medarbejderens primære ansættelse. Hvis det ikke er muligt at finde
   en leder, vil integrationen standse med en `ManagerNotUniqueFromCprException`.

 * --create-user MO_uuid

   Eksempel: python ad_writer-py --create-user 4931ddb6-5084-45d6-9fb2-52ff33998005

   Som ovenfor men i dette tilfælde oprettes der ikke et link til lederens AD konto.

 * --sync-user MO_uuid

   Eksempel: python ad_writer-py --sync-user 4931ddb6-5084-45d6-9fb2-52ff33998005

   Synkroiser oplysninger fra MO til en allerede eksisterende AD konto.

 * --delete-user User_SAM

   Eksempel: python ad_writer-py --delete-user MGORE

   Slet den pågældende AD bruger. Denne funktion anvendes hovedsageligt til tests,
   da et driftmiljø typisk vil have en mere kompliceret procedure for sletning af
   brugere.

 * --read-ad-information User_SAM

   Eksempel: python ad_writer-py --read-ad-information MGORE

   Returnere de AD oplysninger fra AD som integrationen i øjeblikket er konfigureret
   til at læse. Det er altså en delmængde af disse oplysninger som vil blive
   skrevet til MO af synkroniseringsværktøjet. Funktionen er primært nyttig til
   udvikling og fejlfinding.

 * --add-manager-to-user Manager_SAM User_SAM

   Eksempel: python ad_writer-py --add-manager-to-user DMILL MGORE

   Udfylder brugerens ``manager`` felt med et link til AD kontoen der hører til
   ManagerSAM.


execute_ad_script.py
++++++++++++++++++++

Dette værktøj har følgende muligheder:

::

   usage: execute_ad_script.py [-h]
                               [--validate-script Script name |
			       --execute-script Script name user_uuid]

De forskellige muligheder gennemgås her en ad gangen:
 * --validate-script Script_name

   Eksempel: python ad_writer-py --validate-script send_email

   Denne kommando vil lede efter en skabelon i ``scripts/send_email.ps_template`` og
   validere at skabelonen kun indeholder gyldige nøgleværdier. Hvis dette er
   tilfældet returneres sætningen "Script is valid" og ellers returneres en
   fejlbesked som beskriver hvilke ugyldige nøgler der er fundet i skabelonen.

 * --execute-script Script name user_uuid
   Eksempel: python execute_ad_script.py --execute-script send_email 4931ddb6-5084-45d6-9fb2-52ff33998005

   Denne kommando vil finde en skabelon i ``scripts/send_email.ps_template`` og først
   validere og derefter afvikle de med værdier taget fra brugen med uuid som angivet.
