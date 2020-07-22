import os
import pathlib
import json

# flake8: noqa

settingsfile = pathlib.Path("settings") / "settings.json"
top_settings = json.loads(settingsfile.read_text())


settings = {
        "MOX_LOG_LEVEL": top_settings ["os2sync.log_level"],
        "MOX_LOG_FILE": top_settings["os2sync.log_file"],
        "OS2MO_SERVICE_URL": top_settings["mora.base"] + "/service",
        "OS2MO_SAML_TOKEN": top_settings.get("crontab.SAML_TOKEN"),
        "OS2MO_ORG_UUID": "", # dont set - probed unless set
        "OS2MO_HAS_KLE": "", # dont set - probed only
        "OS2MO_CA_BUNDLE": top_settings["os2sync.ca_verify_os2mo"],
        "OS2SYNC_HASH_CACHE": top_settings["os2sync.hash_cache"],  # in CWD
        "OS2SYNC_API_URL": top_settings["os2sync.api_url"],  # http://some-os2sync-url/api/v1_1",
        "OS2SYNC_CA_BUNDLE": top_settings["os2sync.ca_verify_os2sync"],
        "OS2SYNC_PHONE_SCOPE_CLASSES": top_settings["os2sync.phone_scope_classes"],  # can be empty
        "OS2SYNC_EMAIL_SCOPE_CLASSES": top_settings["os2sync.email_scope_classes"],  # can be empty
        "OS2MO_TOP_UNIT_UUID": top_settings["os2sync.top_unit_uuid"],
        "OS2SYNC_MUNICIPALITY": top_settings["municipality.cvr"],
        "OS2SYNC_XFER_CPR": top_settings["os2sync.xfer_cpr"],
}

loggername = "os2sync"
