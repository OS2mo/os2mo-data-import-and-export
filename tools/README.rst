******************
Tools
******************
Tools indeholder scripts primært beregnet til at køre natlige jobs og restore af data efter fejlede jobs.

* job-runner.sh - excutable beregnet til at blive kaldt fra crontab uden parametre
* clear_mox_tables.py - beregnet til at tømme os2mo's tabeller ved nyindlæsning
* cron-restore.sh - beregnet til at restore OS2MO til før den kørsel, som backuppen er taget efter
* moxklas.sh - beregnet til at oprette klasser i LORA - udenom OS2MO ved specialle behov
* prefixed_settings.sh - beregnet til at eksportere settings fra en JSON-fil og ind i current shell environment
* renew-keytab.sh - beregnet til at oprette/genskabe keytabs
* update-dipex.sh - beregnet til at opdatere dette git-repo med ny kode, requirements etc


job-runner.sh
=============

Afvikling af cron-jobs
++++++++++++++++++++++

Job runner scriptet er ment til at blive kaldt fra crontab på kundens maskiner
Dets arbejde er:

* at læse konfigurationen fra settings/settings.json, som er et symbolsk link til settings-filen for systemet.
* at køre de prædefinerede dele af nattens cronjob forudsat at de er slået til i konfigurationen
* at lave en backup af databasen og andre filer, der skal i spil for at få systemet tilbage til en veldefineret tilstand

Læsning af konfiguration
^^^^^^^^^^^^^^^^^^^^^^^^

Konfigurationen kan se således ud:

.. code-block:: json

    {
        "crontab.SVC_USER": "USER@KOMMUNE.NET", 
        "crontab.SVC_KEYTAB": "/path/keytab-file", 
        "crontab.CRON_BACKUP": "/path/backup-dir", 
        "crontab.CRON_LOG_FILE": "/path/cron-log-file", 
        "crontab.RUN_MOX_DB_CLEAR": false,
        "crontab.RUN_CHECK_AD_CONNECTIVITY": false,
        "crontab.RUN_BALLERUP_APOS": false,
        "crontab.RUN_BALLERUP_UDVALG": false,
        "crontab.RUN_QUERIES_BALLERUP": false,
        "crontab.RUN_SD_CHANGED_AT": false,
        "crontab.RUN_SD_FIX_DEPARTMENTS": false,
        "crontab.RUN_SD_DB_OVERVIEW": false,
        "crontab.RUN_AD_SYNC": false,
        "crontab.RUN_MOX_STS_ORGSYNC": false,
        "crontab.RUN_MOX_ROLLE": false,
        "crontab.RUN_CPR_UUID": false,
        "crontab.BACKUP_SAVE_DAYS": "60",
        "crontab.MOX_ROLLE_COMPOSE_YML":"",
        "crontab.SNAPSHOT_LORA":"/path/db-snapshot.sql"
    }


En konfiguration som ovenstående kører ingen jobs, laver en backup i 
``/path/backup-dir`` og sletter gamle backupper, når de er mere end ``60`` dage gamle.
Det bruger AD-kontoen ``USER@KOMMUNE.NET`` når den skal connecte til AD og logger ind 
med ``/path/keytab-file``, når det behøves og logger progress til ``/path/cron-log-file``.

For at enable importen fra SD sættes ``crontab.RUN_SD_CHANGED_AT`` til ``true``.

For at enable exporten til STS Organisation, sættes ``crontab.RUN_MOX_STS_ORGSYNC`` til ``true``.

For at enable exporten to Rollekataloget, sættes ``crontab.RUN_MOX_ROLLE`` til ``true``
og ``crontab.MOX_ROLLE_COMPOSE_YML`` udfyldes med stien til den gældende docker-compose.yml 
file for Rollekatalogseksporten.

Ideen er at dette script kan kaldes fra cron, finder sin egen konfiguration, kører programmerne, hvorefter det
laver en backup af ``/path/db-snapshot.sql`` og andre filer, der er nødvendige 
for at komme tilbage til en veldefineret tilstand.

Der kan være mere konfiguration nødvendig for de enkelte jobs - se disse for detaljer

Kørsel af jobs
^^^^^^^^^^^^^^

job-runner.sh er ikke et smart program. Dert er til gengæld simpelt.: Job-afviklingen foregår i 3 afdelinger: imports, exports og reports.

* For alle jobs i imports og exports gælder at et fejlet job stopper afviklingen af de resterende jobs i den pågældfende afdeling
* Hvis imports går galt, afvikles hverken exports eller reports
* Hvis imports går godt forsøges både exports or reports afviklet

I ovenstående konfiguration kan man slå jobs til med alle de tilgængeglige ``crontab.RUN_*``, som dækker over:

        RUN_MOX_DB_CLEAR : 			Tøm OS2mo's database
        RUN_CHECK_AD_CONNECTIVITY: 		Check at der er di korrekte rettigheder i AD
        RUN_BALLERUP_APOS			Indlæs til OS2MO fra APOS (Ballerups version)
        RUN_BALLERUP_UDVALG			Indlæs udvalgshierarkiet i Ballerups OS2MO
        RUN_QUERIES_BALLERUP			Kør Ballerups exports / forespørgsler
        RUN_SD_CHANGED_AT			Kør SD-changed-at
        RUN_SD_FIX_DEPARTMENTS			Kør SD-fix-departments
        RUN_SD_DB_OVERVIEW			Få et overblik over SD-indlæsningens progress (datoer)
        RUN_AD_SYNC				Kør en AD-synkronisering
        RUN_MOX_STS_ORGSYNC			Kør Overførslen til STS Organisation
        RUN_MOX_ROLLE				Kør overførslen til rollekataloget
        RUN_CPR_UUID				Lav en cachefile med CPR/UUID-sammenhænger - gøres typisk før en genindlæsning


Pakning og lagring af Backup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Filer til backup er angivet i 3 afdelinger (bash-arrays):

* BACK_UP_BEFORE_JOBS - fil lagres i backup inden kørslen af de enablede jobs afvikles
* BACK_UP_AFTER_JOBS - fil lagres i backup efter at kørslen af de enablede jobs er afviklet
* BACK_UP_AND_TRUNCATE - fil lagres i backup efter at kørslen af de enablede jobs er afviklet, hvorefter fil trunkeres til størrelse 0. Dette er praktisk til logfiler, som nu pakkes sammen med det datagrundlag, der skal til for at gentage kørslen.

Pakning af backup foregår i to afdelinger:

* pre_backup - her pakkes alle filer i BACK_UP_BEFORE_JOBS sammen i en tidsstemplet tarfil
* post_backup - her pakkes filerne i BACK_UP_AFTER_JOBS og BACK_UP_AND_TRUNCATE ned i tarfilen, som gzippes og filerne i BACK_UP_AND_TRUNCATE trunkeres. 

Lagringen af backup foregår i servicebrugerens hjemmedirectory, se ``crontab.CRON_BACKUP`` i konfigurationseksemplet ovenfor.


Afvikling af et enkelt job udenom cron
++++++++++++++++++++++++++++++++++++++

Det kan, for eksempel under udfikling eller test, være nødvendigt at afvikle en kørsel 'i hånden'
Den mulighed har man også med job-runner scriptet.  Man giver simpelhen navnet på den indre funktion med i kaldet.

Herefter læses konfiguration på normal vis, men der tages nu ikke hensyn til om jobbet er slået til i konfigurationen eller ej, det køres

Følgende interne funktioner kan kaldes:

* imports_test_ad_connectivity
* imports_sd_fix_departments
* imports_sd_changed_at
* imports_ad_sync
* exports_mox_rollekatalog
* exports_mox_stsorgsync
* reports_sd_db_overview
* reports_cpr_uuid

Vil man for eksempel afvikle mox_stsorgsync, anvender man kaldet:

    tools/jon-runner.sh exports_mox_stsorgsync



clear_mox_tables.py
===================

Dette anvendes typisk kun af cron-restore og der, hvor man hver nat genindlæser OS2mo fra APOS.

cron-restore.sh
===============

Tømmer OS2MOS database og indlæser backup i mo og pakker run-db ud. 'Run-db er en lille sqlite-database, som fortæller SD-changed-at hvor langt den er kommet.

moxklas.sh
==========

Anvendes under implementering til at oprette klasser i Lora.


prefixed_settings.sh
====================

prefixed_settings sources og anvender to environment-variable, med følgende defaults:

.. code-block:: bash

    export SETTING_PREFIX=${SETTING_PREFIX:=crontab}
    export CUSTOMER_SETTINGS=${CUSTOMER_SETTINGS:=/opt/settings/customer-settings.json}

Det omsætter værdier fra ovenstående konfigurationsfil, fjerner et prefix og eksporterer værdierne

Med øverststående konfigurationsfil ville der efter en sourcing af scriptet eksistere en nøgle SVC_USER i environment med værdien USER@KOMMUNE.NET


renew-keytab.sh
===============

Dette interaktive program gør det muligt med lidt trial-and-error, når man skal have frembragt en brugbar keytab-fil.

update-dipex.sh
===============

Dette program anvendes for at opdatere repositoriet og afhængigheder

