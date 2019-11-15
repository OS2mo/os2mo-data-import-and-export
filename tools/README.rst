******************
Tools
******************
Tools indeholder scripts primært beregnet til at køre natlige jobs og restore af data efter fejlede jobs.

Job runner 
==========
Job runner scriptet er ment til at blive kaldt fra crontab på kundens maskiner
Dets arbejde er:

* at læse konfigurationen  det gøres fra settings/settings.json, som er et symbolsk link til settings-filen for systemet.
* at køre de prædefinerede dele af nattens cronjob forudsat at de er slået til i konfigurationen
* at lave en backup af databasen og andre filer, der skal i spil for at få systemet tilbage til en veldefineret tilstand

Konfigurationen kan se således ud:

.. code-block:: json

    {
        "crontab.SVC_USER": "USER@KOMMUNE.NET", 
        "crontab.SVC_KEYTAB": "/path/keytab-file", 
        "crontab.CRON_BACKUP": "/path/backup-dir", 
        "crontab.CRON_LOG_FILE": "/path/cron-log-file", 
        "crontab.RUN_CHECK_AD_CONNECTIVITY": false,
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

Afvikling af et enkelt job
==========================

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
