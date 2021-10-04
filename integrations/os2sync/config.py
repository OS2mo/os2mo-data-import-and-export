import json
import logging
import os

from customer_settings import get_settings
from customer_settings import PathDefaultMethod

# flake8: noqa

settingsfile = get_settings(PathDefaultMethod.raw)
top_settings = json.loads(settingsfile.read_text())


def _relpath(filename):
    return os.path.join(os.getcwd(), filename)


settings = {
    # Fields without defaults
    "OS2MO_CA_BUNDLE": top_settings["os2sync.ca_verify_os2mo"],
    "OS2MO_TOP_UNIT_UUID": top_settings["os2sync.top_unit_uuid"],
    "OS2SYNC_CA_BUNDLE": top_settings["os2sync.ca_verify_os2sync"],
    "OS2SYNC_MUNICIPALITY": top_settings["municipality.cvr"],
    # Fields with defaults
    "MOX_LOG_LEVEL": top_settings.get("os2sync.log_level", logging.INFO),
    "MOX_LOG_FILE": top_settings.get("os2sync.log_file", _relpath("os2sync.log")),
    "OS2MO_SERVICE_URL": (
        top_settings.get("mora.base", "http://localhost:5000") + "/service"
    ),
    "OS2MO_SAML_TOKEN": top_settings.get("crontab.SAML_TOKEN"),
    "OS2MO_ORG_UUID": "",  # dont set - probed unless set
    "OS2MO_HAS_KLE": "",  # dont set - probed only
    "OS2SYNC_HASH_CACHE": top_settings.get(
        "os2sync.hash_cache", _relpath("os2sync_hash_cache")
    ),
    "OS2SYNC_API_URL": top_settings.get("os2sync.api_url", "http://localhost:8081"),
    "OS2SYNC_XFER_CPR": top_settings.get("os2sync.xfer_cpr", False),
    "OS2SYNC_USE_LC_DB": top_settings.get("os2sync.use_lc_db", False),
    "OS2SYNC_PHONE_SCOPE_CLASSES": top_settings.get("os2sync.phone_scope_classes", []),
    "OS2SYNC_EMAIL_SCOPE_CLASSES": top_settings.get("os2sync.email_scope_classes", []),
    "OS2SYNC_IGNORED_UNIT_LEVELS": top_settings.get("os2sync.ignored.unit_levels", []),
    "OS2SYNC_IGNORED_UNIT_TYPES": top_settings.get("os2sync.ignored.unit_types", []),
    "OS2SYNC_AUTOWASH": top_settings.get("os2sync.autowash", False),
    "OS2SYNC_TEMPLATES": top_settings.get("os2sync.templates", {}),
    "use_contact_for_tasks": top_settings.get("os2sync.use_contact_for_tasks"),
    "sync_managers": top_settings.get("os2sync.sync_managers")
    "os2sync.employee_engagement_address": top_settings.get("os2sync.employee_engagement_address", False),
}

logformat = "%(levelname)s %(asctime)s %(name)s %(message)s"
loggername = "os2sync"
