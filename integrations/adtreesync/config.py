from ra_utils.load_settings import load_settings

from ..ad_integration.read_ad_conf_settings import read_settings  # type: ignore


def get_ldap_settings():
    dipex_settings = load_settings()
    ad_settings = read_settings()
    return {
        "domain": dipex_settings["integrations.adtreesync"]["domain"],
        "servers": dipex_settings["integrations.adtreesync"]["servers"],
        "user": ad_settings["primary"]["system_user"],
        "password": ad_settings["primary"]["password"],
        "cpr_attribute": ad_settings["primary"]["cpr_field"],
        "search_base": ad_settings["primary"]["search_base"],
    }
