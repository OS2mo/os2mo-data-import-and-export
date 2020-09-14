import json
import os
import pathlib

# flake8: noqa

settingsfile = pathlib.Path("settings") / "settings.json"
top_settings = json.loads(settingsfile.read_text())


settings = {
    "MORA_BASE" : top_settings.get("mora.base", 'http://localhost:5000'),
    "MORA_ROOT_ORG_UNIT_UUID" : top_settings.get("mora.admin_top_unit"),
    "USERID_ITSYSTEM" : top_settings["emus.userid_itsystem"],
    "EMUS_RESPONSIBILITY_CLASS" : top_settings["emus.manager_responsibility_class"],
    "EMUS_FILENAME" : top_settings.get("emus.outfile_name", 'emus_filename.xml'),
    "EMUS_DISCARDED_JOB_FUNCTIONS" : top_settings.get("emus.discard_job_functions",[]),
    "EMUS_DISCARDED_ADDRESS_VISIBILITY_CLASSES" : top_settings.get("emus.discarded_address_visibility_classes",[]),
    "EMUS_ALLOWED_ENGAGEMENT_TYPES" : top_settings.get("emus.engagement_types",[]),
    "EMUS_USE_LC_DB" : top_settings.get("emus.use_lc_db",[]),
}

logformat = '%(levelname)s %(asctime)s %(name)s %(message)s'
logfile = 'emus_log.txt'
loggername = "emus"
loglevel = 10

