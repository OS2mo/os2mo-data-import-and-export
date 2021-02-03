import json
import os
import pathlib

# flake8: noqa

# TODO: Refactor this
settingsfile = pathlib.Path("settings") / "settings.json"
top_settings = json.loads(settingsfile.read_text())


settings = {
    "MORA_ROOT_ORG_UNIT_UUID": top_settings.get("mora.admin_top_unit"),
    "EMUS_RESPONSIBILITY_CLASS": top_settings["emus.manager_responsibility_class"],
    "EMUS_FILENAME": top_settings.get("emus.outfile_name", 'emus_filename.xml'),
    "EMUS_DISCARDED_JOB_FUNCTIONS": top_settings.get("emus.discard_job_functions", []),
    "EMUS_ALLOWED_ENGAGEMENT_TYPES": top_settings.get("emus.engagement_types", []),
    "EMUS_PHONE_PRIORITY": top_settings.get("emus.phone.priority", []),
    "EMUS_EMAIL_PRIORITY": top_settings.get("emus.email.priority", []),
}

logformat = '%(levelname)s %(asctime)s %(name)s %(message)s'
logfile = 'emus_log.txt'
loggername = "emus"
loglevel = 10

