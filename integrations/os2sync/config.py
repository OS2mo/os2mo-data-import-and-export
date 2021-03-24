import json

# flake8: noqa
from customer_settings import PathDefaultMethod, get_settings

settingsfile = get_settings(PathDefaultMethod.raw)

top_settings = json.loads(settingsfile.read_text())

settings = {
        "MOX_LOG_LEVEL": top_settings["os2sync.log_level"],
        "MOX_LOG_FILE": top_settings["os2sync.log_file"],
        "OS2MO_SERVICE_URL": top_settings["mora.base"] + "/service",
        "OS2MO_SAML_TOKEN": top_settings.get("crontab.SAML_TOKEN"),
        "OS2MO_ORG_UUID": "",  # dont set - probed unless set
        "OS2MO_HAS_KLE": "",  # dont set - probed only
        "OS2MO_CA_BUNDLE": top_settings["os2sync.ca_verify_os2mo"],
        "OS2SYNC_HASH_CACHE": top_settings["os2sync.hash_cache"],  # in CWD
        "OS2SYNC_API_URL": top_settings["os2sync.api_url"],
        # http://some-os2sync-url/api/v1_1",
        "OS2SYNC_CA_BUNDLE": top_settings["os2sync.ca_verify_os2sync"],
        "OS2SYNC_PHONE_SCOPE_CLASSES": top_settings["os2sync.phone_scope_classes"],
        # can be empty
        "OS2SYNC_EMAIL_SCOPE_CLASSES": top_settings["os2sync.email_scope_classes"],
        # can be empty
        "OS2MO_TOP_UNIT_UUID": top_settings["os2sync.top_unit_uuid"],
        "OS2SYNC_MUNICIPALITY": top_settings["municipality.cvr"],
        "OS2SYNC_XFER_CPR": top_settings["os2sync.xfer_cpr"],
        "OS2SYNC_USE_LC_DB": top_settings.get("os2sync.use_lc_db", False),
        "OS2SYNC_IGNORED_UNIT_LEVELS": top_settings.get("os2sync.ignored.unit_levels",
                                                        []),
        "OS2SYNC_IGNORED_UNIT_TYPES": top_settings.get("os2sync.ignored.unit_types",
                                                       []),
        "OS2SYNC_AUTOWASH": top_settings.get("os2sync.autowash", False),
}
logformat = '%(levelname)s %(asctime)s %(name)s %(message)s'
loggername = "os2sync"
