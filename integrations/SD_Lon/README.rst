**********************
Integration til SD Løn
**********************

Indledning
==========
Denne integration gør det muligt at hente og opdatere organisations- og
medarbejderoplysninger fra SD Løn til OS2MO. 

.. _SD løn opsætning:

Opsætning
==========

For at kunne afvikle integrationen, kræves loginoplysninger til SD-Løn, som angives
via ``settings.json``, desuden anvendes en række felter som angiver den lokale anvendelse
af SD Løn. De påkrævede felter er:

 * ``integrations.SD_Lon.institution_identifier``: Institution Identifer i SD.
 * ``integrations.SD_Lon.sd_user``: Brugernavn (inklusiv foranstillet SY) til SD.
 * ``integrations.SD_Lon.sd_password``: Password til SD.
 * ``integrations.SD_Lon.base_url``: url til SD's webinterface.
 * ``integrations.SD_Lon.global_from_date``: Virkningsdato for import på formen YYYY-MM-DD.
 * ``integrations.SD_Lon.import.too_deep``: Liste over SD niveauer som anses som
   afdelingsniveau.
 * ``integrations.SD_Lon.monthly_hourly_divide``: Skilleværdi for måneds/timelønnede.
 * ``integrations.SD_Lon.job_function``: Feltet kan have en af to vædier:
   `EmploymentName` eller `JobPositionIdentifier`, se yderligere nedenfor.

Desuden kan disse ikke-påkrævede felter angives:

 * ``integrations.SD_Lon.employment_field``: Angiver et af MOs ekstrafelter på
   engagementer, hvis feltet angives vil integrationen skrive værdien af
   `EmploymentName` i dette felt.
 * ``integrations.SD_Lon.skip_employment_types``: En liste over værdier af
   `JobPositionIdentifier` som ikke skal importeres. Hvis et engagement har
   en type fra listen, vil engagementet bliver ignoreret og ikke importeret i MO.
   Den tilhørende bruger vil dog blive oprettet, men vil optræde uden engagementer
   (med mindre personen har andre engagementer i kommunen).
 * ``integrations.SD_Lon.no_salary_minimum_id``: Angiver en minimum påkrævet job
   position id for ulønnede medarbejdere. Alle ulønnede medarbejder med et id
   under dette minimum får aldrig deres engagement oprettet i MO.
 * ``integrations.SD_Lon.fix_departments_root``: Angiver hvilken org_unit som skal
   udgøre rodenhed for importerede organisationenheder fra SD. Hvis tom anvendes
   MO's rodorganisation.

Hvis ``integrations.SD_Lon.job_function`` har værdien `EmploymentName` vil
ansættelsers stillingsbetegnelser bliver taget fra SDs felt af samme navn, som
er et fritekstfelt. Integrationen vil oprette en klasse for alle forekommende
stillingsbetegnelser.
Benyttes i stedet værdien `JobPositionIdentifier` vil stillingsbetegelsen blive
taget fra dette felt i SD, som er et klassicieret felt.

Desuden er det nødvendigt at angive adressen på MO og LoRa i variablerne:

 * ``mox.base``
 * ``mora.base``

Brug af integrationen
=====================
De forskellige underprogrammer kan alle tilgåes igennem ét hoved program,
nemlig ``sd_cli``, ved kørsel af dette program vises underprogrammerne, og
deres parametre og formål kan udforskes. Kør blot:
```
python integrations/SD_Lon/sd_cli.py --help
```

Detaljer om importen
====================
Udtræk fra SD Løn foregår som udgangspunkt via disse webservices:

 * ``GetOrganization20111201``
 * ``GetDepartment20111201``
 * ``GetPerson20111201``
 * ``GetEmployment20111201``
  
Det er desuden muligt at køre et udtræk som synkroniserer ændringer som er meldt ind
til SD Løn, men endnu ikke har nået sin virkningsdato:

 * ``GetEmploymentChanged20111201``
 * ``GetPersonChangedAtDate20111201``

Endelig er der også en implementering af løbende synkronisering af ændringer i SD
Løn, til dette anvendes udover de nævne webservices også:

 * ``GetEmploymentChangedAtDate20111201``

Hvis der ønskes synkronisering af titler hørende til ``JobPositionIdentifier``
anvendes desuden:

 * ``GetProfession20080201``

Alle enheder fra SD importeres 1:1 som de er i SD, dog er det muligt at flytte enheder
uden hverken overenhed eller underenheder til en særlig overenhed kaldet
'Forældreløse enheder'.

Medarbejdere som er ansat på niveauerne, angivet i konfigurationensnøglen
``integrations.SD_Lon.import.too_deep`` rykkes op til det laveste niveau højere end
dette, og der oprettes en tilknytning til den afdeling de befinder sig i i SD Løn.

Det er muligt at levere en ekstern liste med ledere, eller alternativt at benytte SD
Løns JobPositionIdentifier til at vurdere at en medarbejder skal regnes som leder.

Medarbejdere i statuskode 3 regnes for at være på orlov.

Der importeres ingen adresser på medarbejdere. Disse kan eventuelt hentes fra
ad-integrationen.

Alle personer og ansættelser som kan returneres fra de ovennævnte webservices
importeres, både passive og aktive. Dette skyldes dels et ønske om et så komplet
datasæt som muligt, dels at SDs vedligeholdsesservices gå ud fra, at alle kendte
engagementer er i den lokale model.

Den importerede startdato for engagementer er desværre ikke i alle tilfælde korrekt,
men repræsenterer for aktive ansættelser den dato hvor den nuværende
ansættelsesstatus indtrådte, da det ikke er muligt at finde den korrekte oprindelige
startdato uden et meget stort antal kald mod SDs api.
For afsluttede ansættelser vil sidste ændrede status være lig med slutdatoen, i
disse tilfælde anvendes i stedet SDs felt EmploymentDate, som desværre er et
fritekstfelt som i pricippet kan være behæftet med fejl.

Postadresser på enheder hentes fa SD og valideres mod DAR. Hvis adressen kan entydigt
genkendes hos DAR, gemmes den tilhørende DAR-uuid på enheden i MO.

Email adresser og p-numre importeres fra SD hvis disse findes for enheden.

Vi importerer UUID'er på enheder fra SD til MO så enheder i MO og SD har samme UUID.

Medarbejdere har ikke en UUID i SD, så her benyttes cpr som nøgle på personen og
ansættelsesnummeret som nøgle på engagementer. Brugerens UUID i MO vil enten blive
tilfældigt valgt, eller trukket fra eksternt givet liste som matcher cpr-numre med
ønskede UUID'er i MO. Denne funktionalitet kan anvendes til at sikre, at brugere
ikke skifter UUID hvis det bliver nødvendigt at genimporere fra SD. TIl hjælp til
dette findes et script (``cpr_uuid.py``) under exports som kan lave en sådan liste
fra en kørende instans af MO.

Engagementstyper
================

Alle medarbejdere som har et ansættelsesnummer udelukkende med tal, tildeles en af
to ansættelsestyper:

 * Medarbejder (månedsløn), hvis ansættelsesnummeret er lavere end værdien angivet
   i nøglen ``integrations.SD_Lon.monthly_hourly_divide``.
 * Medarbejder (timeløn), hvis ansættelsesnummeret er højere.

Hvis medarbejderen har et ansættelsesnummer, som ikke udelukkende er tal, vil
ansættelsestypen blive bestemt fra personens ``JobPositionIdentifier``, hvor der i
MO er oprettet klasser der svarer til disse værdier. Den tilknyttede tekst til hver
klasse kan sættes med et hjælpeværktøj (beskrevet nedenfor).

Primær ansættelse
=================

SD Løn har ikke et koncept om primæransættelse, men da AD integrationen til MO
har behov for at kunne genkende den primære ansættelse til synkronisering, bestemmes
dette ud fra en beregning:

En medarbejders primære ansættelse regnes som den ansættelse som har den største
arbejdstidsprocent, hvis flere har den samme, vælges ansættelsen med det laveste
ansættelsenummer. Hvis en ansættelse er manuelt angivet til at være primær, vil
denne ansættelse altid regnes som primær.

Ansættelser i SDs statuskode 0 kan anses som primære hvis ingen andre ansættelser
er primære (altså, medarbejderen har udelukkende ansættelser i statuskode 0).
Hvis en medarbejder har ansættelser i både status 0 og status 1, vil en ansættelse
i status 1 blive beregnet til primær og status 0 ansættelsen vil ikke blive
betragtet som primær.

Informationen om primæransætelse opretholdes i MOs facet ``primary_type``, som ved
import af SD altid populeres med disse fire klasser:

 * Manuelt primær ansættelse: Dette felt angiver at en ansættelse manuelt er sat
   til at være primær
 * Ansat: Angiver en medarbejders beregnede primære ansættelse.
 * Ansat - Ikke i løn: Angiver SD Løns statuskode 0. Hvis ingen andre primære
   ansætelser findes vil denne type regnes som primær.
 * Ikke-primær ansat: Angiver alle andre ansættelser for en medarbejder.

Manuelt primær optræder ikke direkte i imports, men kan sættes manuelt fra MOs GUI.
De øvrige primærklasser håndteres af SD integrationen, og må ikke sættes manuelt.

En medarbejder skifter ikke ansættelsestype selvom vedkommende fratræder sit
engagement. En ansættelses aktuelle status angives i stedet via MOs start- og
slutdato. Er slutdato'en i fortiden, er vedkommende ikke længere ansat og vil
i MOs gui fremgå i fanen fortid. Er en medarbejers startdato i fremtiden, er
personen endnu ikke tiltrådt, og fremgår i fanen fremtid.
.. _Håndtering af enheder:


Håndtering af enheder
=====================
SDs API til udlæsning af organisationsenheder er desværre meget mangelfuldt, og
integrationen har derfor en yderst primitiv håndtering af enheder:

Ved førstegangsimport vil alle aktuelle enheder blive importeret med den
virkningstid som oplyses af kald til ``GetDepartment``. Dette er dog ikke
nødvendigvis den egentlige oprettelsesdato for enheden og der vil være tilfælde
hvor startdato er enten for tidlig eller for sen i forhold til den reele startdato
for enheden.

Der findes ikke nogen differentiel service fra SD som oplyser om ændringer i
organisationen, og der sker derfor som udgangspunkt ingen synkronisering af
enhedstræet mellem SD og MO. I de tilfælde hvor der ansættes en medarbejder i en
enhed som enten ikke eksisterer i MO, eller hvor enhedens virkningstid er kortere
end ansættelsens start, vil MO oprette enheden eller forlænge dens virkningstid
så den bliver i stand til at rumme engagementet.

Da det er meget vanskeligt at hente historisk information om enheder, vil MO oprette
eller rette enheden med udgangspunkt i de data som gælder for enheden på importdagen.
Enheden vil herefter fremgå af MO som om den altid har haft det navn og den placering
den har på importdagen.

Hvis en enhed omdøbes eller flyttes i SD, vil denne ændring ikke fremgå af MO, med
mindre der foretages en manuel synkronisering, dette kan gøres ved at at afvikle
scriptet ``fix_departments.py``, hvis kommunen ønsker det, er det muligt at slå en
funktionalitet til som tillader denne afvikling via en knap i MOs front-end.

Når ``fix_departments.py`` afvikles på en enhed, vil enheden og dens forældres
navne og hierakiske placering blive hentet fra SD og den nye tilstand vil blive
skrevet til MO med evig virkning både bagud og fremad i tid.
Hvis enhedens niveau er angivet i ``integrations.SD_Lon.import.too_deep`` til at være
et afdelingsnieau vil integrationen desuden genberegne placeringen de engagementer
som SD har registreret på enheden som vil blive flyttet opad til det laveste
strukturniveau i undertræet. Denne flytning vil få en registreret virkningstid som
er lig med den dag  ``fix_departments.py`` blev afviklet.

Det skal altså understreges, at MOs historiske information om enhder **ikke** er
retvisende. Det betyder dels, at det ikke er muligt at se tidligere navne på
enheden, men mere bemærkelsesværdigt er det, at det ikke er muligt at se tidligere
placeringer i organisationshierakiet. Det betyder altså, at enheden potentielt
tidligere kan have været placeret et helt andet sted i organisationen. Hvis en
medarbejder har været ansat i en enhed mens enheden er er blevet flyttet, vil dette
ikke fremgå at medarbejderens fortidsfane, da engagementets tilknytning til enheden
ikke har været ændret. Det er derfor vigtigt at holde sig for øje, at selvom en
medarbejders historik ikke indeholder ændringer i organisatorisk placering, kan
vedkommende godt være flyttet alligevel i form af eventuelle flytninger af hele
enheden.

I tilknytning til SD importen, er der i øjeblikket ved at blive implementeret en
funktionalitet som via SD Løns beskedservice kan oprette enheder i SD når de
oprettes i MO. Med denne service vil den fremadrettede historik for enheder fra
idriftsættelsen af servicen, blive korrekt.


Hjælpeværktøjer
===============
Udover de direkte værktøjer til import og løbende opdateringer, findes et antal
hjælpeværktøjer:

 * ``test_sd_connectivity.py``: Et lille værktøj som tester at den lokale
   ``settings.json`` indeholder de nødvendige nøgler. Desuden tester programmet
   for en række potentielle fejl, eksempevis om felterne har gyldige værdier
   og om det er muligt at kontakte SD Løn med de angivne brugeroplysinger.

 * ``test_mo_against_sd.py``: Et værktøj som tester udvalgte personers engagementer
   mod SD løn of checker at MO og SD er løn har samme opfattelse af om personens
   engagementer er aktive eller ej. Værktøjet kan anvendes på et enkelt person
   eller på alle personer som har ansættelse i en bestemt enhed (alle engagementer
   for disse personer vil blive tjekket også dem i andre enheder). Værktøjet
   anvender opslag til SDs API'er og kan derfor kun anvendes i begrænset omfang, og
   af samme årsag er der ikke implementeret mulighed for at tjekke alle ansatte.

 * ``calculate_primary.py``: Et værktøj som er i stand til at gennemløbe alle
   ansættelser i MO og afgøre om der for alle medarbejdere til alle tider
   findes et primærengagement. Værktøjet er også i stand til at reparere en
   (eller alle) ansættelser hvor dette ikke skulle være tilfældet. Dette modul
   importeres desuden af koden til løbende opdatering, hvor den bruges til at
   genberegne primæransættelser når der skær ændringer i en medarbejders
   ansættelsesforhold.
   Værktøjet er udstyret med et kommandolinjeinterface, som kan udskrive en liste
   over brugere uden primærengagement (eller med mere end et) samt opdatere
   primære engagementer for en enkelt bruger eller for alle brugere.

 * ``sync_job_id.py``: Dette værktøj kan opdatere den tekst som vises i forbindelse
   med ansættelsestyper og stillingsbetegnelser som er knyttet til SDs
   ``JobPositionIdentifier``. Efter den initielle import vil klassens navn modsvare
   talværdien i SD, og dette værktøj kan efterfølgende anvendes til at enten at
   synkronisere teksten til den aktuelle værdi i SD eller til en valgfri tekst.

 * ``fix_departments.py``: En implementering af logikken beskrevet under afsnitet
   `Håndtering af enheder`_. Udover anvendelsen i den løbende integrationen,
   indeholder programmet også et kommandolinjeværktøj som kan anvendes til
   manuelt at fremprovokere en synkronisering af en enhed (med tilhørende
   overenheder) til den nuværende tilsand af SD Løn.
   Hvis værktøjet afvikles på en enhed som anses for at være Afdelings-niveau,
   vil det opdatere alle enhedens ansættelser, så engagementerne flyttes til
   de korrekte NY-niveauer (som kan være ændret, hvis afdelingen er flyttet).

 * ``sd_fix_organisation.py``: Tidligere forsøg på at håndtere opdateringer af
   enheder. Scriptet findes nu kun som basis for evenutelle senere forsøg på
   at lave et fuldt historisk import af enhedstræet.


Tjekliste for fuldt import
==========================
Overordnet foregår opstart af en ny SD import efter dette mønster:

1. Kør importværktøjet med fuld historik (dette er standard opførsel).
2. Kør en indledende ChangedAt for at hente alle kendte fremtidige ændringer og
   intitialisere den lokale database over kørsler.
3. Kør sd_changed_at.py periodisk (eksempelvis dagligt).
4. Eventuelt synkronisering af stillingsbetegnelser.
5. Eventuelt synkronisering fra AD.
   
1. Kør importværktøjet
----------------------
En indledende import køres ved at oprette en instans af ImportHelper_ ImportHelper

.. code-block:: python

    importer = ImportHelper(
        create_defaults=True,
        mox_base=MOX_BASE,
        mora_base=MORA_BASE,
        store_integration_data=False,
        seperate_names=True
    )
			       
Hverken importen eller efterfølgende synkronisering med ChangedAt anvender
integrationsdata, og det er derfor valgfrit om vil anvende dette.

Importen kan derefter køres med disse trin:

.. code-block:: python

    sd = sd_importer.SdImport(
        importer,
        ad_info=None,
        manager_rows=None
    )

    sd.create_ou_tree(
        create_orphan_container=False,
        sub_tree=None,
        super_unit=None
    )
    sd.create_employees()

    importer.import_all()

Hvor der i dette tilfælde ikke angives ledere eller en AD integration. Disse to
punkter diskuteres under punkterne `Ledere i SD Løn`_ og
`AD Integration til SD Import`_.

Parametren `sub_tree` kan angives med en uuid og det vil så fald kun blive
undertræet med den pågældende uuid i SD som vil blive importeret. Det er i
øjeblikket et krav, at dette træ er på rod-niveau i SD.

Importen vil nu blive afviklet og nogle timer senere vil MO være populeret med
værdierne fra SD Løn som de ser ud dags dato.

2. Kør en indledende ChangedAt
------------------------------
I SD Løn importeres i udgangspunktet kun nuværende og forhenværende medarbejdere og
engagementer, fremtidige ændringer skal hentes i en seperat process. Denne process
håndteres af programmet `sd_changed_at.py` (som også anvendes til efterfølgende
daglige synkroniseringer). Programmet tager i øjeblikket desværre ikke mod parametre
fra kommandolinjen, men har brug for at blive rettet direkte i koden, hvor parametren
`init` i `__main__` delen af programmet skal sættes til `True`.

Programet kan nu afvikles direkte fra kommandolinjen

python3 sd_changed_at.py

Herefter vil alle kendte fremtidige virkninger blive indlæst til MO. Desuden vil der
blive oprettet en sqlite database med en oversigt over kørsler af changed_at (se
ChangedAt.db_) .

3. Kør sd_changed_at.py periodisk
---------------------------------

Daglige indlæsninger foregår som nævnt også med programmet `sd_changed_at.py`,
hvilket foregår ved at sætte `init` til `False` og køre programmet uden yderligere
parametre. Programmet vil så spørge ChangedAt.db_ om hvorår der sidst blev
synkroniseret, og vil herefter synkronisere yderligere en dag frem i tiden.

4. Eventuelt synkroisering af stillingsbetegnelser
--------------------------------------------------

Hvis nøglen  * ``integrations.SD_Lon.job_function`` er valgt til `JobPositionIdentifier`,
vil alle stillingsbetegnelser nu være talværdier fra SD Løns klassificerede
stillinger, for at få læsbare stillinger skal disse synkroniseres ved hjælp af
værktøjet ``sync_job_id.py`` (se ovenfor).


5. Eventuelt synkronisering fra AD
----------------------------------

Hvis det ønskes at synkronisere adresser fra AD, skal scriptet ``ad_sync.py``
afvikles, settings til dette er beskrevet i afsnittet `Integration til Active Directory`_


.. _Ledere i SD Løn:

Ledere
======

SD Løn indeholder som udgangspunkt ikke information om, hvorvidt en ansat er leder. Det er
derfor ikke muligt importere informaion om ledere direke fra dataudtrækket. Der er
dog implementeret to metoder til at angive lederinformation:

 1. Inddirekte via `JobPositionIdentifier`

    Det er muligt at angive et antal værdier for `JobPositionIdentifier` som anses
    for at være ledere. Disse er i øjeblikket hårdkodet til værdierne 1030, 1040 og
    1050. Hvis intet andet angives vil disse medarbejdere anses for at være ledere i
    de afdelinger de er ansat i.

 2. Via eksternt leveret fil.

    Integrationen understøtter at blive leveret en liste af ledere som kan importeres
    fra en anden kilde. Denne liste angives med parametren ``manager_rows`` ved
    opstart af importeren. Formatet for denne anivelse er

    .. code-block:: python

        manager_rows = [

	    {'cpr': leders_cpr_nummer,
	     'ansvar': 'Lederansvar'
	     'afdeling': sd_enhedskode
	    }
	    ...
        ]

    Hvor lederansvar er en fritekststreng, alle unikke værdier vil blive oprettet
    under facetten ``responsibility`` i Klassifikation. Det er i den nuværende
    udgave ikke muligt at importere mere end et lederansvar pr leder.

.. _AD Integration til SD import:

AD Integration til SD import
============================
SD Importen understøtter at anvende komponenten
`Integration til Active Directory`_ til at berige objekterne fra SD Løn med
information fra Active Directory. I de fleste tilfælde drejer dette sig som minimum
om felterne ``ObjectGuid`` og  ``SamAccountName`` men det er også muligt at hente
eksempelvis telefonnumre eller stillingsbetegnelser.

Feltet ``ObjectGuid`` vil i MO blive anvendt til UUID for det tilhørende
medarbejderobjekt, hvis ikke UUID'en allerede er givet fra en ekstern kilde.
``SamAccountName`` vil blive tilføjet som et brugernavn til IT systemet Active
Direkctory for den pågældende bruger.

.. _ChangedAt.db:

ChangedAt.db
============

For at holde rede på hvornår MO sidst er opdateret fra SD Løn, findes en SQLite
database som indeholder to rækker for hver færdiggjort kørsel. Adressen på denne
database er angivet i settings med nøglen ``integrations.SD_Lon.import.run_db``.

Programmet ``db_overview.py`` er i stand til at læse denne database og giver et
outut som dette:

::

   id: 1, from: 2019-08-22 00:00:00, to: 2019-08-22 00:00:00, status: Running since 2019-08-22 14:03:01.226492
   id: 2, from: 2019-08-22 00:00:00, to: 2019-08-22 00:00:00, status: Initial import: 2019-08-22 16:31:29.151569
   id: 3, from: 2019-08-22 00:00:00, to: 2019-08-23 00:00:00, status: Running since 2019-08-23 09:00:04.215068
   id: 4, from: 2019-08-22 00:00:00, to: 2019-08-23 00:00:00, status: Update finished: 2019-08-23 09:05:36.587527
   id: 5, from: 2019-08-23 00:00:00, to: 2019-08-24 00:00:00, status: Running since 2019-08-28 08:44:11.181134
   id: 6, from: 2019-08-23 00:00:00, to: 2019-08-24 00:00:00, status: Update finished: 2019-08-28 08:46:19.146615
   id: 7, from: 2019-08-24 00:00:00, to: 2019-08-25 00:00:00, status: Running since 2019-08-28 08:49:27.479475
   id: 8, from: 2019-08-24 00:00:00, to: 2019-08-25 00:00:00, status: Update finished: 2019-08-28 08:49:36.189767
   id: 9, from: 2019-08-25 00:00:00, to: 2019-08-26 00:00:00, status: Running since 2019-08-28 08:50:42.929468
   id: 10, from: 2019-08-25 00:00:00, to: 2019-08-26 00:00:00, status: Update finished: 2019-08-28 08:50:51.811845
   id: 11, from: 2019-08-26 00:00:00, to: 2019-08-27 00:00:00, status: Running since 2019-08-28 08:54:46.207228
   id: 12, from: 2019-08-26 00:00:00, to: 2019-08-27 00:00:00, status: Update finished: 2019-08-28 08:59:20.876762
   id: 13, from: 2019-08-27 00:00:00, to: 2019-08-28 00:00:00, status: Running since 2019-08-28 09:07:25.961710
   id: 14, from: 2019-08-27 00:00:00, to: 2019-08-28 00:00:00, status: Update finished: 2019-08-28 09:12:08.191701

Ved starten af alle changedAt kørsler, skrives en linje med status ``Running`` og
efter hver kørsel skrives en linje med status ``Update finished``.  En changedAt
kørsel kan ikke startes hvis den nyeste linje har status ``Running``, da dette
enten betyder at integrationen allerede kører, eller at den seste kørsel fejlede.


.. _SD_MOX:

SD_MOX
======

SD-MOX er en udvidelse til SD-løn, som leveres af Silkeborg Data. SD-Mox muliggør 
opdatering af visse felter på organisationsenheder, som findes både i OS2MO
og i SD-løn.

OS2MO's integration til SD-mox involverer brug af SD-løns AMQP-kø
til afsendelse af ændringer og oprettelser, hvorimod læsning og verifikation foregår
via SD's webinterface.

Integrationen er synkron,  udført med triggere (se https://os2mo.readthedocs.io/en/latest/dev/triggers.html), 
således at forstå at man får svar umiddelbart i forbindelse med sin handling, som er en af

1. oprettelse af organisatiorisk enhed
2. omdøbning af organisatorisk enhed
3. flytning af organisatorisk enhed
4. ændring/oprettelse af adresser på en organisatorisk enhed


.. _SD_MOX_konfiguration:

Konfiguration
-------------

Konfiguration af modulet er fleksibel og dermed lidt kompleks. For det første
er der url, bruger og password mv. til SD's webinterface som dokumenteret under
`SD løn opsætning`_ . SD's AMQP-opsætning er derimod specifik for SD-mox-modulet 
og udgøres af disse settings:

 * ``integrations.SD_Lon.sd_mox.AMQP_USER``: AMQP bruger aftalt med SD
 * ``integrations.SD_Lon.sd_mox.AMQP_HOST``: AMQP host aftalt med SD
 * ``integrations.SD_Lon.sd_mox.AMQP_PORT``: AMQP port aftalt med SD
 * ``integrations.SD_Lon.sd_mox.AMQP_PASSWORD``: AMQP password aftalt med SD
 * ``integrations.SD_Lon.sd_mox.AMQP_CHECK_RETRIES``: Antal gange man prøver at validere de via AMQP overførte ændringer (default: 6)
 * ``integrations.SD_Lon.sd_mox.AMQP_CHECK_WAITTIME``: Ventetid før hvert forsøg på validering (default: 3)
 * ``integrations.SD_Lon.sd_mox.VIRTUAL_HOST``: Virtuel host aftalt med SD

Dernæst beskriver ``integrations.SD_Lon.sd_mox.TRIGGERED_UUIDS`` en liste af 
UUID-strenge for afdelinger på topniveau, som, inklusive undertræer, anses som 
forbundet med SD. Den kan se ud som ``["e3e38b32-61c0-4900-a200-000001510002"]``,
flere strenge adskilles af komma.

SD-løn anvender et begreb, som hedder NY-Niveauer, disse er reguleret sådan at man kan
sætte en afdeling på ``Afdelings-niveau`` ind under en afdeling på ``NY1-niveau``, men 
ikke omvendt. ``integrations.SD_Lon.sd_mox.OU_LEVELKEYS`` beskriver en liste af 
NY-niveauer i rækkefølge fra højere til lavere niveauer. Denne liste anvendes til at
omsætte os2mos klasse-uuider for facetten ``org_unit_level`` til de tekst-strenge som 
SD-MOX forventer samt for at validere omtalte regler inden indsætning. Den ser 
typisk ud som ``["NY6-niveau", "NY5-niveau",... ,"NY1-niveau", "Afdelings-niveau"]``

Nogle kommuner anvender en facet, der hedder ``time_planning``, og den setting, 
der hedder ``integrations.SD_Lon.sd_mox.OU_TIME_PLANNING_MO_VS_SD`` udgør en mapning
imellem brugervendte nøgler for klasserne i ``time_planning`` og de strenge, der skal 
overføres til SD som repræsentation for samme. Den kan se ud som :
``{..., "DannesIkke": "Normaltjeneste dannes ikke"}``


Anvendelse af SD-mox 
--------------------

Når man i OS2MOS grafiske klient arbejder med organisatoriske enheder i et undertræ,
der er inkluderet i ``integrations.SD_Lon.sd_mox.TRIGGERED_UUIDS``, vil flytninger,
oprettelser, omdøbninger og tilføjelse/ændring af adresser bliver overført til SD. 
Der er dog visse begræsninger i input, som gennemgås nedenfor. 

Der er en forsinkelse på 8.5 sekunder i brugerinterfacet mellem afsendelse imod SD
og modtagelse af kvitteringen for ændringerne. Det er ikke SD, som har den forsinkelse; 
Den er indført i OS2MO fordi vi ikke får kvitteringen for ændringen direkte fra SD,
men først ser den via at opslag på webinterfacet, og ei er nødt til at vente til vi 
forventer at SD er faldet til ro efter en ændring.


Begrænsninger i input
---------------------

Der er en del begrænsninger i input, som er indført enten ud fra viden om SD’s krav eller
slet og ret ved at prøve sig frem. Alle disse begrænsninger gælder kun i de OS2MO-undertræer, 
som er inkluderet i ``integrations.SD_Lon.sd_mox.TRIGGERED_UUIDS``

 * Afdelingsnumre skal være med stort. Det er de hos SD.

 * Pnumre efter addresser. Det interface vi anvender hos SD kan kun vise Pnumre
   hvis der er en postadresse – derfor har vi indført et krav om postadresse, 
   hvis man angiver Pnummer.

 * Afdelingsnumre skal være 2 til 4 karakterer lange i SD - denne begrænsning understøttes
   af SD-MOX

 * Ny Niveauer har ikke-tilladte forældre-barn-relationer, og der valideres inden vi forsøger
   at sætte noget ind hos SD. 


SD-mox fejlmeddelelser
----------------------

Der er en del mulige fejl, man kan begå, når man anvender OS2MO med denne integration tilkoblet.
Der er gjort et stort arbejde for at fange dem, så man ikke kan lave en ændring i OS2MO,
der ikke er reflekteret i SD. Der vises fejlmeddelser i OS2MO's brugerinterface for at gøre 
opmærksom på dem og de er alle foranstillet prefixet ``Integrationsfejl, SD-Mox:``

 * SD AMQP credentials mangler
 * Klasse-uuider for conf af Ny-Niveauer eller Tidsregistrering mangler
 * Uventet svar fra SD amqp
 * Startdato skal altid være den første i en måned
 * Afdeling ikke unik. Code {}, uuid {}, level {}
 * Enhedsnummer for kort
 * Enhedsnummer for langt
 * Ugyldigt tegn i enhedsnummer
 * Enhedsnummer skal være store bogstaver
 * Enhedsnummer er i brug
 * Forældrenheden findes ikke
 * Enhedstypen passer ikke til forældreenheden
 * Afdeling ikke fundet: %s
 * Følgende felter kunne ikke opdateres i SD
 * Enhedstype er ikke et kendt NY-niveau
 * Forældreenhedens enhedstype er ikke et kendt NY-niveau
 * Opret postaddresse før pnummer

