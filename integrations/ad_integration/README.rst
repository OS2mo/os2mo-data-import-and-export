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

Det er muligt at anvende flere AD til udlæsning af adresser og itsystemer til OS2MO
Således er ``integrations.ad`` i ``settings.json`` et array med følgende 
indbyggede betydning:
 
 * Første AD i listen (index 0) anvendes til skrivning (hvis skrivning er aktiveret) 
   og til integrationer, som endnu ikke er forberedt for flere ad'er.

 * Alle AD'er anvendes af ad_sync til opdatering af og skabelse af adresser, itsystemer 




Fælles parametre
----------------

 * ``integrations.ad.winrm_host``: Hostname på remote mangagent server

For hvert ad angives
-----------

 * ``search_base``: Search base, eksempelvis
   'OU=enheder,DC=kommune,DC=local'
 * ``cpr_field``: Navnet på feltet i AD som indeholder cpr nummer.
 * ``cpr_separator``: Angiver en eventuel separator mellem
   fødselsdato og løbenumre i cpr-feltet i AD. Hvis der ikke er en separator,
   angives en tom streng.
 * ``sam_filter``: Hvis denne værdi er sat, vil kun det være muligt
   at cpr-fremsøge medarbejder som har denne værdi foranstillet i SAM-navn.
   Funktionen muliggør at skelne mellem brugere og servicebrugere som har samme
   cpr-nummer.
 * ``caseless_samname``: Hvis denne værdi er ``true`` (Default) vil sam_filter 
   ikke se forskel på store og små bogstaver.
 * ``system_user``: Navnet på den systembruger som har rettighed til
   at læse fra AD.
 * ``password``: Password til samme systembruger.
 * ``properties``: Liste over felter som skal læses fra AD. Angives
   som en liste i json-filen.
 * ``servers`` - domain controllere for denne ad.


Test af opsætningen
-------------------

Der følger med AD integrationen et lille program, ``test_connectivity.py`` som tester
om der er oprettet de nødvendige Kerberos tokens og konfiguration. Programmet
afvikles med en af to parametre:

 * ``--test-read-settings``
 * ``--test-write-settings``

En test af læsning foregår i flere trin:
 * Der testes for om Remote Management serveren kan nås og autentificeres med et
   kereros token.
 * Der testes om det er muligt af afvikle en triviel kommando på AD serveren.
 * Der testes for, at en søgning på alle cpr-numre fra 31. november returnerer
   nul resultater.
 * Der testes for, at en søging på cpr-numre fra den 30. i alle måneder returnerer
   mindst et resultat. Hvis der ikke returneres nogen, er fejlen efter sandsynligt
   en manglende rettighed til at læse cpr-nummer feltet.
 * Der testes om de returnerede svar indeholder mindst et eksempel på disse tegn:
   æ, ø, å, @ som en test af at tegnsættet er korrekt sat op.

En test af skrivning foregår efter denne opskrift:

 * Der testes for om de nødvendige værdier er til stede i ``settings.json``, det
   drejer sig om nøglerne:
   * ``integrations.ad.write.uuid_field``: AD feltet som rummer MOs bruger-UUID
   * ``integrations.ad.write.level2orgunit_field``: AD feltet hvor MO skriver
   den primære organisatoriske gruppering (direktørområde, forvaltning, etc.)
   for brugerens primære engagement.
   * ``integrations.ad.write.org_unit_field``: Navnet på det felt i AD, hvor MO
   skriver enhedshierakiet for den enhed, hvor medarbejderen har sin primære
   ansættelse.
   * ``integrations.ad.write.upn_end``: Endelse for feltet UPN.
   * ``integrations.ad.write.level2orgunit_type``: UUID på den klasse som beskriver
   at en enhed er den primære organisatoriske gruppering (direktørområde,
   forvaltning, etc.). Dette kan være en enhedstype eller et enhedsniveau.

 * Der udrages et antal tilfældige brugere fra AD (mindst 10), og disse tjekkes for
   tilstædeværelsen af de tre AD felter beskrevet i
   ``integrations.ad.write.uuid_field``,
   ``integrations.ad.write.level2orgunit_field`` og
   ``integrations.ad.write.org_unit_field``. Hvis hvert felt findes hos mindst
   en bruger, godkendes den lokale AD opsætning.
 * Længden af cpr-numrene hos de tilfældige brugere testes for om de har den
   forventede længde, 10 cifre hvis der ikke anvendes en separator, 11 hvis der
   gør. Det er et krav for at integrationen kan køre korrekt, at alle cpr-numre
   anvender samme (eller ingen) separator.

Hvis disse tests går igennem, anses opsætningen for at være klar til
AD skriv integrationen.

   
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


Valg af primær konto ved flere konti pr. cprnummer
--------------------------------------------------

Nogle steder har man flere konti med samme cprnummer i AD'et.
For at vælge den primære, som opdaterer / opdateres fra MO,
kan man anvende et sæt nøgler i settingsfilen:

  * ``integrations.ad.discriminator.field`` et felt i det pågældende AD, som bruges til at
afgøre hvorvidt denne konto er den primære
  * ``integrations.ad.discriminator.values`` et sæt strenge,
som matches imod ``integrations.ad.discriminator field``
  * ``integrations.ad.discriminator.function`` kan være 'include' eller 'exclude'

Man definerer et felt, som indeholder en indikator for om kontoen er den primære,
det kunnne f.x være et felt, man kaldte xBrugertype, som kunne indeholde "Medarbejder".

Hvis man i dette tilfælde sætter ``integrations.ad.discriminator.function``
til ``include`` vil kontoen opfattes som primær hvis 'Medarbejder' også findes i
``integrations.ad.discriminator.values``

Opfattes mere end en konto som primær sættes programmet til at fejle.

Findes nøglen ``integrations.ad.discriminator.field``, skal de andre to nøgler
også være der. Findes den ikke, opfattes alle AD-konti som primære.


Skrivning til AD
================

Der udvikles i øjeblikket en udvidelse til AD integrationen som skal muliggøre at
oprette AD brugere og skrive information fra MO til relevante felter.

Hvis denne funktionalitet skal benyttes, er der brug for yderligere parametre som
skal være sat når programmet afvikles:

 * ``integrations.ad.write.servers``: Liste med de DC'ere som findes i kommunens AD.
   Denne liste anvendes til at sikre at replikering er færdiggjort før der skrives
   til en nyoprettet bruger.
 * ``integrations.ad.write.uuid_field``: Navnet på det felt i AD, hvor MOs
   bruger-uuid skrives.
 * ``integrations.ad.write.level2orgunit_field``: Navnet på det felt i AD, hvor MO
   skriver navnet på den oganisatoreiske hovedgruppering (Magistrat, direktørområde,
   eller forvalting) hvor medarbejderen har sin primære ansættelse.
 * ``integrations.ad.write.org_unit_field``: Navnet på det felt i AD, hvor MO
   skriver enhedshierakiet for den enhed, hvor medarbejderen har sin primære
   ansættelse.
 * ``integrations.ad.write.primary_types``: Sorteret lister over uuid'er på de
   ansættelsestyper som markerer en primær ansættelse. Jo tidligere et engagement
   står i listen, jo mere primært anses det for at være.
 * ``integrations.ad.write.level2orgunit_type``: uuid på den enhedstype som angiver
   at enheden er en organisatorisk hovedgruppering og derfor skal skrives i feltet
   angivet i ``integrations.ad.write.level2orgunit_field``.
 * ``integrations.ad.write.create_user_trees``: Liste over uuid'er på enheder,
   medarbejdere i disse enheder samt deres underheder, vil få oprettet AD en
   konto af scriptet `ad_life_cycle.py` hvis de ikke har en i forvejen.


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

AD til MO
+++++++++

Synkronisering fra AD til MO foregår via programmet ``ad_sync.py``.

Programmet opdaterer alle værdier i MO i henhold til den feltmapning som er angivet
i `settings.json`. Det er muligt at synkronisere adresseoplysninger, samt at
oprette et IT-system på brugeren, hvis brugeren findes i AD, men endnu ikke har et
tilknyttet IT-system i MO. Desuden er det muligt at synkronisere et AD felt til
et felt på brugerens primærengagement (typisk stillingsbetegnelsen). 
Husk at efterfølgende AD kan overskrive. Derfor:
Anvend ikke samme klasser, itsystemer eller extensionfelter i flere af 
de specificerede AD'er

Et eksempel på en feltmapning angives herunder:

.. code-block:: json

    "ad_mo_sync_mapping": {
        "user_addresses": {
            "telephoneNumber": ["a6dbb837-5fca-4f05-b369-8476a35e0a95", "INTERNAL"],
            "pager": ["d9cd7a04-a992-4b31-9534-f375eba2f1f4 ", "PUBLIC"],
            "EmailAddress": ["fbd70da1-ad2e-4373-bb4f-2a431b308bf1", null],
            "mobile": ["6e7131a0-de91-4346-8607-9da1b576fc2a ", "PUBLIC"]
        },
        "it_systems": {
            "samAccountName": "d2998fa8-9d0f-4a2c-b80e-c754c72ef094"
        },
        "engagements": {
            "Title": "extension_2"
        }
    }

For adresser angives en synlighed, som kan antage værdien `PUBLIC`, `INTERNAL`,
`SECRET` eller `null` som angiver at synligheden i MO sættes til henholdsvis
offentlig, intern, hemmelig, eller ikke angivet. UUID'er er på de tilhørende
adresseklasser i MO som AD felterne skal mappes til.

Hvis der for en given bruger er felter i feltmapningen som ikke findes i AD, vil
disse felter bliver sprunget over, men de øvrige felter vil stadig blive
synkroniseret.

Selve synkroniseringen foregår ved at programmet først udtrækker samtlige
medarbejdere fra MO, der itereres hen over denne liste, og information fra AD'et
slås op med cpr-nummer som nøgle. Hvis brugeren findes i AD, udlæses alle parametre
angivet i `integrations.ad.properties` og de af dem som figurerer i feltmapningen
synkroniseres til MO.

Integrationen vil som udgangspunkt ikke synkronisere fra et eventuelt skole AD, med
mindre nøglen `integrations.ad.skip_school_ad_to_mo` sættes til `false`.

Da AD ikke understøtter gyldighedstider, antages alle informationer uddraget fra AD
at gælde fra 'i dag' og til evig tid. Den eneste undtagelse til dette er ved
afslutning af deaktiverede AD brugere.

Deaktiverede AD brugere kan håndteres på forskellige måder.
Som udgangspunkt synkroniseres de på præcis samme vis som almindelige brugere,
med mindre nøglen `integrations.ad.ad_mo_sync_terminate_disabled` er sat til `True`.
Hvis dette er tilfælde ophører den automatiske synkronisering, og deaktiverede
brugere får deres AD data 'afsluttet'.
Ved afslutning forstås at brugerens AD synkroniserede adresser og it-systemer
flyttes til fortiden, såfremt de har en åben slutdato.

Slutteligt skal det nævnes, at implemeneringen af synkroniseringen understøtter
muligheden for at opnå en betydelig hastighedsforbering ved at tillade direkte adgang
til LoRa, denne funktion aktiveres med nøglen
`integrations.ad.ad_mo_sync_direct_lora_speedup` og reducerer kørselstiden
betragteligt. Hvis der er få ændringer vil afviklingstiden komme ned på nogle få
minutter.

MO til AD
+++++++++

Synkronisering fra MO til AD foregår efter en algoritme hvor der itereres hen over
alle AD brugere. Hver enkelt bruger slås op i MO via feltet angivet i nøglen
`integrations.ad.write.uuid_field` og informatione fra MO synkroniseres
til AD i henhold til den lokale feltmapning. AD-integrationen stiller et antal
værdier til rådighed, som det er muligt at synkronisere til felter i AD. Flere
kan tilføjes efterhånden som integrationen udvikles.

 * ``employment_number``: Lønsystemets ansættelsesnummer for medarbejderens primære
   engagement.
 * ``end_date``: Slutdato for længste ansættelse i MO, hvis en ansættelse ikke har
   nogen kendt slutdato, angives 9999-12-31.
 * ``uuid``: Brugerens UUID i MO.
 * ``title``: Stillingsbetegnelse for brugerens primære engagement.
 * ``unit``: Navn på enheden for brugerens primære engagement.
 * ``unit_uuid``: UUID på enheden for brugerens primære engagement.
 * ``unit_user_key``: Brugervendt nøgle for enheden for brugerens primære engagement,
   dette vil typisk være lønssystemets kortnavn for enheden.
 * ``unit_public_email``: Email på brugerens primære enhed med synligheen ``offentlig``
 * ``unit_secure_email``: Email på brugerens primære enhed med synligheen ``hemmelig``.
   Hvis enheden kun har email-adresser uden angivet synlighed, vil den blive agivet
   her.
 * ``unit_postal_code``: Postnummer for brugerens primære enhed.
 * ``unit_city``: By for brugerens primære enhed.
 * ``unit_streetname``: Gadenavn for brugerens primære enhed.
 * ``location``: Fuld organisatorisk sti til brugerens primære enhed.
 * ``level2orgunit``: Den oganisatoreiske hovedgruppering (Magistrat, direktørområde,
   eller forvalting) som brugerens primære engagement hører under.
 * ``manager_name``: Navn på leder for brugerens primære engagement.
 * ``manager_cpr``: CPR på leder for brugerens primære engagement.
 * ``manager_sam``: SamAccountName for leder for brugerens primære engagement.
 * ``manager_mail``: Email på lederen for brugerens primære engagement.

Felterne ``level2orgunit`` og ``location`` synkroniseres altid til felterne angivet i
nøglerner ``integrations.ad.write.level2orgunit_type`` og
``integrations.ad.write.org_unit_field``, og skal derfor ikke specificeres yderligere
i feltmapningen.

Desuden synkroniseres  altid AD felterne:
 * `Displayname`: Synkroniseres til medarbejderens fulde navn
 * `GivenName`: Synkroniseres til medarbejderens fornavn
 * `SurName`: Synkroniseres til medarbejderens efternavn
 * `Name`: Synkroniseres til vædien
   "`Givenname`  `Surname`  - `Sam_account_name`"
 * `EmployeeNumber`: Synkroniseres til `employment_number`

Yderligere synkronisering fortages i henhold til en lokal feltmaping, som eksempelvis
kan se ud som dette:

.. code-block:: json

   "integrations.ad_writer.mo_to_ad_fields": {
	"unit_postal_code": "postalCode",
	"unit_city": "l",
	"unit_user_key": "department",
	"unit_streetname": "streetAddress",
	"unit_public_email": "extensionAttribute3",
	"title": "Title",
	"unit": "extensionAttribute2"
   }

Formattet for denne skal læses som: MO felt --> AD felt, altså mappes
`unit_public_email` fra MO til `extensionAttribute3` i AD.

Som et alternativ til denne direkte 1-til-1 felt-mapning er der mulighed for en
mere fleksibel mapning vha. `jinja` skabeloner (Se eventuelt her:
https://jinja.palletsprojects.com/en/2.11.x/templates/ (Engelsk)).

Brug af jinja skabelon for AD feltmapning, kan eksempelvis se ud som dette:

.. code-block:: json

   "integrations.ad_writer.template_to_ad_fields": {
	"postalCode": "{{ mo_values['unit_postal_code'] }}",
	"department": "{{ mo_values['unit_user_key'] }}",
	"streetName": "{{ mo_values['unit_streetname'].split(' ')[0] }}",
    "extensionAttribute3": "{{ mo_values['unit_public_email']|default('all@afdeling.dk') }}",
   }

Det er værd at bemærke at begge systemer; `mo_to_ad_fields` og
`template_to_ad_fields` benytter jinja systemet i maven på eksporteren.

Det er altså ækvivalent at skrive henholdvis:

.. code-block:: json

   "integrations.ad_writer.mo_to_ad_fields": {
	"unit_postal_code": "postalCode",
   }

og:

.. code-block:: json

   "integrations.ad_writer.template_to_ad_fields": {
	"postalCode": "{{ mo_values['unit_postal_code'] }}",
   }

Da førstnævnte konverteres til sidstnævnte internt i programmet.


Afvikling af PowerShell templates
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
eksempel på sådan en fil kunne se sådan ud:

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
       "integrations.ad.write.level2orgunit_type": "cdd1305d-ee6a-45ec-9652-44b2b720395f",
       "integrations.ad.write.primary_types": [
	   "62e175e9-9173-4885-994b-9815a712bf42",
	   "829ad880-c0b7-4f9e-8ef7-c682fb356077",
	   "35c5804e-a9f8-496e-aa1d-4433cc38eb02"
       ],
       "integrations.ad_writer.mo_to_ad_fields": {
	   "unit_user_key": "department",
	   "level2orgunit": "company",
	   "title": "Title",
	   "unit": "extensionAttribute2"
       },
       "integrations.ad.write.uuid_field": "sts_field",
       "integrations.ad.write.level2orgunit_field": "extensionAttribute1",
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
	   "physicalDeliveryOfficeName",
	   "extensionAttribute1",
	   "extensionAttribute2",
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


Hvor betydniningen af de enkelte felter er angivet højere oppe i dokumentationen.
Felter som omhandler skolemdomænet er foreløbig sat via miljøvariable og er ikke
inkluderet her, da ingen af skriveintegrationerne på dette tidspunkt undestøtter
dette.

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
 * ``ad_life_cycle.py``
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


ad_life_cycle.py
++++++++++++++++

Dette værktøj kan afhængig af de valgte parametre oprette eller deaktivere AD-konti
på brugere som henholdsvis findes i MO men ikke i AD, eller findes i AD, men ikke
har aktive engagementer i MO.

::
   usage: ad_life_cycle.py [-h]
                           [--create-ad-accounts] [--disable-ad-accounts]
                           [--dry-run]

Betydningen af disse parametre angives herunder, det er muligt at afvilke begge
synkroniseringer i samme kørsel ved at angive begge parametre.
			   
 * --create-ad-accounts

   Opret AD brugere til MO brugere som ikke i forvejen findes i AD efter de
   regler som er angivet i settings-nøglen
   ``integrations.ad.write.create_user_trees``.

 * --disable-ad-accounts

   Sæt status til Disabled for AD konti hvor den tilhøende MO bruge ikke længere
   har et aktivt engagement.
			   
 * --dry-run

   Programmet vil ikke forsøge at opdatere sit billede af MO, en vil anvende
   den aktuelt cache'de værdi. Dette kan være nyttigt til udvikling, eller
   hvis flere integrationer køres umidelbart efter hinanden.

   
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


Import af AD OU til MO
======================

Som en ekstra funktionalitet, er det muligt at anvende AD integrationens
læsefaciliteter til at indlæse en bestemt OU fra AD'et til MO. Dette vil eksempelvis
kunne anvendes hvis AD'et er autoritativ for eksterne konsulenter i kommunen og man
ønsker, at disse personer skal fremgå af MOs frontend på trods af at de ikke
importeres fra lønsystemet.
Integrationen vil oprette ansættelsestypen 'Ekstern' og vil oprette alle brugere fra
et på forhånd angivet OU som ansatte i MO. Det er en forudsætning, at disse brugere
ikke har andre ansættelser i MO i forvejen. Hvis brugere fjernes fra OU'et vil de
blive fjernet fra MO ved næste kørsel af integrationen.

I den nuværende udgave af integrationen, genkendes OU'et med eksterne brugere på,
at dets navn indeholder ordene 'Ekstern Konsulenter', dette vil på sigt blive
erstattet med konfiguration.

For at programmet kan afvikles, er det nødvendigt at sætte konfigurationsværdien
``integrations.ad.import_ou.mo_unit_uuid`` som angiver UUID'en på den enhed brugerne
fra AD skal synkroniseres til. Hvis enheden ikke eksisterer i forvejen vil
den blive oprettet ved første kørsel, så for en kommune som starter op med brug af
denne integration, kan der blot angives et tilfældigt UUID.

Programmet hedder ``import_ad_group_into_mo.py`` og kan anvendes med et antal
kommandolinjeparametre:

 *   --create-or-update: Opretter og opdaterer bruger fra AD til MO.
 *   --cleanup-removed-users: Fjerne MO brugere som ikke længere er konsulenter i AD.
 *   --full-sync: Kører begge de to ovenstående operationer.
