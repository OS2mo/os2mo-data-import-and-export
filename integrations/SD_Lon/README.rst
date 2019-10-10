**********************
Integration til SD Løn
**********************

Indledning
==========
Denne integration gør det muligt at hente og opdatere organisations- og
medarbejderoplysninger fra SD Løn til OS2MO. 

Opsætning
==========

For at kunne afvikle integrationen, kræves loginoplysninger til SD-Løn, som angives
via miljøvariable i den terminal integrationen afvikles fra. Disse miljøvariable er:

 * ``INSTITUTION_IDENTIFIER``: Institution Identifer i SD.
 * ``SD_USER``: Brugernavn (inklusiv foranstillet SY) til SD.
 * ``SD_PASSWORD``: Password til SD.

Desuden er det nødvendigt at angive adressen på MO og LoRa i variablerne:
 * ``MOX_BASE``
 * ``MORA_BASE``

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
  
Alle enheder fra SD importeres 1:1 som de er i SD, dog er det muligt at flytte enheder
uden hverken overenhed eller underenheder til en særlig overenhed kaldet
'Forældreløse enheder'.

Medarbejdere som er ansat på niveauerne, Afdelings-niveau og NY1-niveau rykkes op til
det laveste niveau højere end dette, og der oprettes en tilknytning til den afdeling
de befinder sig i i SD Løn.

Det er muligt at levere en ekstern liste med ledere, eller alternativt at benytte SD
Løns JobPositionIdentifier til at vurdere at en medarbejder skal regnes som leder.

Medarbejdere i statuskode 3 regnes for at være på orlov.

Der importeres ingen adresser på medarbejdere. På sigt vil disse kunne hentes fra
ad-integrationen.

Alle personer og ansættelser som kan returneres fra de ovennævnte webservices
importeres, både passive og aktive. Dette skyldes dels et ønske om et så komplet
datasæt som muligt, dels at SDs vedligeholdsesservices gå ud fra, at alle kendte
engagementer er i den lokale model.

Postadresser på enheder hentes fa SD og valideres mod DAR. Hvis adressen kan entydigt
genkendes hos DAR, gemmes den tilhørende DAR-uuid på enheden i MO.

Email adresser og p-numre importeres fra SD hvis disse findes for enheden.

Vi importerer UUID'er på enheder fra SD til MO så enheder i MO og SD har samme UUID.

Medarbejdere har ikke en UUID i SD, så her benyttes cpr som nøgle på personen og
ansættelsesnummeret som nøgle på engagementer

Primær ansættelse
=================

SD Løn har ikke et koncept om primæransættelse, men da AD integrationen til MO
har behov for at kunne genkende den primære ansættelse til synkronisering, bestemmes
dette ud fra en beregning:
En medarbejders primære ansættelse regnes som den ansættelse som har den største
arbejdstidsprocent, hvis flere har den samme, vælges ansættelsen med det laveste
ansættelsenummer. Hvis en ansættelse er manuelt angivet til at være primær, vil
denne ansættelse altid regnes som primær.

Ansættelser i SDs status kode 0 kan anses som primære hvis ingen andre ansættelser
er primære (altså, medarbejderen har udelukkende ansættelser i status kode 0).
Hvis en medarbejder har ansættelser i både status 0 og status 1, vil en ansættelse
i status 1 blive beregnet til primær og status 0 ansættelsen vil ikke blive
betragtet som primær.

MOs betegnelse 'Primær' anvedes ikke af SD integrationen, da dette felt ikke
automatisk synkroniseres med ansættelsestypen, feltet bør derfor ikke benyttes.
Hvis den aktuelle version af MO undestøtter at slå visningen af feltet fra, kan
dette med fordel gøres.


Håndtering af SD Løns statuskoder
=================================
En medarbejder der importers fra SD, kan have en af fire forskelllige ansættelsestyper:

 * Manuelt primær ansættelse: Dette felt angiver at en ansættelse manuelt er sat
   til at være primær
 * Ansat: Angiver en medarbejders beregnede primære ansættelse.
 * Ansat - Ikke i løn: Angiver SD Løns statuskode 0. Hvis ingen andre primære
   ansætelser findes vil denne type regnes som primær.
 * Ikke-primær ansat: Angiver alle andre ansættelser for en medarbejder.

Manuelt primær optræder ikke direkte i imports, men kan sættes manuelt fra MOs GUI.

En medarbejder skifter ikke ansættelsestype selvom vedkommende fratræder sit
engagement. En ansættelses aktuelle status angives i stedet via MOs start- og
slutdato. Er slutdato'en i fortiden, er vedkommende ikke længere ansat og vil
i MOs gui fremgå i fanen fortid. Er en medarbejers startdato i fremtiden, er
personen endnu ikke tiltrådt, og fremgår i fanen fremtid i MOs gui.


Hjælpeværktøjer
===============
Udover de direkte værktøjer til import og løbende opdateringer, findes et antal
hjælpeværktøjer:

 * `sd_fix_organisation.py`: Forsøger at synkronisere alle nye enheder fra SD Løn
   til MO. Der findes ikke nogen differentiel service fra SD som oplyser om
   ændringer i organisationen, så det er nødvendigt at sammenligne alle enheder
   til alle tider for at opnå en komplet synkronisering. Værktøjet er i øjeblikket
   hårdkodet til at hente alle ændringer til organisatinen siden 1. januar 2019.
   På sigt vil dette værktøj formentlig blive erstattet af enten en service som
   opretter enheder efterhånden som der dukker ansatte om i enheder som ikke
   findes i MO (kræver at SDs nye service GetDepartmentParent tages i brug),
   eller af den SD-mox agent som er ved at blive udviklet.

 * `calculate_primary.py`: Et værktøj som er i stand til at gennemløbe alle
   ansættelser i MO og afgøre om der for alle medarbejdere til alle tider
   findes et primærengagement. Værktøjet er også i stand til at reparere en
   (eller alle) ansættelser hvor dette ikke skulle være tilfældet. Dette modul
   importeres desuden af koden til løbende opdatering, hvor den bruges til at
   genberegne primæransættelser når der skær ændringer i en medarbejders
   ansættelsesforhold.
   Værktøjet er udstyret med et kommandolinjeinterface, som kan udskrive en liste
   over brugere uden primærengagement (eller med mere end et) samt opdatere
   primære engagementer for en enkelt bruger eller for alle brugere.

Tjekliste for fuldt import
==========================
Overordnet foregår opstart af en ny SD import efter dette mønster:

1. Kør importværktøjet med fuld historik (dette er standard opførsel).
2. Kør `sd_fix_organisation.py` for at sikre synkronisering af alle enheder
3. Kør en inledende ChangedAt for at hente alle kendte fremtidige ændringer og
   intitialisere den lokale database over kørsler.
4. Kør sd_changed_at.py periodisk (eksempelvis dagligt). Hvis enhederne har ændret
   sig, er det nødvendigt først at køre sd_fix_organisation.py før hver kørsel.

1. Kør importværktøjet
----------------------
En indledende import køres ved at oprette en instans af ImportHelper_ ImportHelper

.. code-block:: python

   importer = ImportHelper(
       create_defaults=True,
       mox_base=MOX_BASE,
       mora_base=MORA_BASE,
       system_name='SD-Import',
       end_marker='SDSTOP',
       store_integration_data=True,
       seperate_names=True
   )
			       
Hverken importen eller efterfølgende synkronisering med ChangedAt anvender
integrationsdata, og det er derfor valgfrit om vil anvende dette.

Importen kan derefter køres med disse trin:

.. code-block:: python

    sd = sd_importer.SdImport(
	importer,
        MUNICIPALTY_NAME,
	MUNICIPALTY_CODE,
        import_date_from=GLOBAL_GET_DATE,
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

2. `sd_fix_organisation.py`
-------------------------------
Den indledende import henter kun enhedsstrukturen for den virkningsdato importen
foretages fra, hvis der er fremtidige ændringer skal disse hentes efterfølgende.
Til det formål findes værktøjet `sd_fix_organisation.py` som henter alle fremtidige
ændringer til organisationen:

python3 sd_fix_organisation.py


3. Kør en inledende ChangedAt
-----------------------------
I SD Løn importeres i udgangspunktet kun nuværende og forhenværende medarbejdere og
engagementer, fremtidige ændringer skal hentes i en seperat process. Denne process
håndteres af programmet `sd_changed_at.py` (som også anvendes til efterfølgende
daglige synkroniseringer). Programmet tager i øjeblikket desværre ikke mod parametre
fra kommandolinjen, men har brug for at blive rettet direkte i koden, hvor parametren
`init` i `__main__` delen af programmet skal sættes til `True`. Desuden skal
`from_date` sætte til samme dato som importen blev foretaget med.

Programet kan nu afvikles direkte fra kommandolinjen

python3 sd_changed_at.py

Herefter vil alle kendte fremtidige virkninger blive indlæst til MO. Desuden vil der
blive oprettet en sqlite database med en oversigt over kørsler af changed_at (se
ChangedAt.db_) .

4. Kør sd_changed_at.py periodisk
---------------------------------

Daglige indlæsninger foregår som nævnt også med programmet `sd_changed_at.py`,
hvilket foregår ved at sætte `init` til `False` og køre programmet uden yderligere
parametre. Programmet vil så spørge ChangedAt.db_ om hvorår der sidst blev
synkroniseret, og vil herefter synkronisere yderligere en dag frem i tiden.

Programmet gør ikke noget forsøg på at opdatere organisationen, og vil fejle hvis
en medarbejder modtager en ansættelse i en ukendt enhed. For at undgå dette skal
man før `sd_changed_at` afvikle `sd_fix_organisation.py` hvis der er oprettet nye
enheder.
   
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

Feltet ``ObjectGuid`` vil i MO blive anvendt til uuid for det tilhørende
medarbejderobjekt. ``SamAccountName`` vil blive tilføjet som et brugernavn til
IT systemet Active Direkctory for den pågældende bruger.

.. _ChangedAt.db:

ChangedAt.db
============

For at holde rede på hvornår MO sidst er opdateret fra SD Løn, findes en SQLite
database som indeholder to rækker for hver færdiggjort kørsel. Adressen på denne
database er angivet i miljøvariablen ``RUN_DB``.

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
