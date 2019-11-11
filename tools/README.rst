******************
Tools
******************
The tools directory holds tools primarily for runnning unattended jobs
and restoring data after job failure.

Job runner
==========
The Job runner script is meant to be the script that is called from crontab on 
customer machines. It features:
* reading configuration - from settings/settings.json, a symbolic link pointing to the settings file for the system
* running the predefined parts of the cronjob if they are enabled in the configuration
* backing up enough files to be able to reach a known state in the lora database after an error

The configuration for the job runner is specified like this in the configuration file

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
        "crontab.RUN_MOX_STS_ORGSYNC": false,
        "crontab.RUN_MOX_ROLLE": false,
        "crontab.RUN_CPR_UUID": false,
        "crontab.BACKUP_SAVE_DAYS": "60",
        "crontab.MOX_ROLLE_COMPOSE_YML":"",
        "crontab.SNAPSHOT_LORA":"/path/db-snapshot.sql"
    }

The above configuration runs no jobs, makes a backup in ``/path/backup-dir`` and deletes old backups 
when they are more than ``60`` days old. It uses the AD account ``USER@KOMMUNE.NET`` in order to connect to ad,
and logs in via the ``/path/keytab-file`` when needed and logs progress to ``/path/cron-log-file``

In order to run the import from SD, set the ``crontab.RUN_SD_CHANGED_AT`` to ``true``

In order to run the export to STS Organisation, set the ``crontab.RUN_MOX_STS_ORGSYNC`` to ``true``

In order to run the export to Rollekataloget, set the ``crontab.RUN_MOX_ROLLE`` to ``true`` 
and fill in the ``crontab.MOX_ROLLE_COMPOSE_YML`` with the path to the right docker-compose.yml file.

The idea is that this script is called from cron, finds it's own configuration, runs the programs and 
finally backs up ``/path/db-snapshot.sql`` and other select files related to the jobs that have been run. 
