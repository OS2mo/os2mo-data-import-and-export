import collections
import logging
from typing import Dict
from typing import Optional

import click
from glom import assign
from ra_utils.load_settings import load_settings


logger = logging.getLogger("AdReader")


def _read_global_settings(top_settings):
    global_settings = {}
    global_settings["mora.base"] = top_settings.get("mora.base")
    global_settings["servers"] = top_settings.get("integrations.ad")[0].get(
        "servers", []
    )
    global_settings["winrm_host"] = top_settings.get("integrations.ad.winrm_host")
    global_settings["system_user"] = top_settings["integrations.ad"][0]["system_user"]
    global_settings["password"] = top_settings["integrations.ad"][0]["password"]
    if not global_settings["winrm_host"]:
        msg = "Missing hostname for remote management server"
        logger.error(msg)
        raise Exception(msg)
    return global_settings


def _read_primary_ad_settings(top_settings, index=0):
    primary_settings = {}

    if top_settings.get("integrations.ad") is None:
        raise Exception("integration.ad settings not found")

    if len(top_settings["integrations.ad"]) < (index + 1):
        raise Exception("ad index %d not found" % index)

    # settings that must be in place
    primary_settings["search_base"] = top_settings["integrations.ad"][index].get(
        "search_base"
    )
    primary_settings["cpr_field"] = top_settings["integrations.ad"][index].get(
        "cpr_field"
    )
    primary_settings["system_user"] = top_settings["integrations.ad"][index].get(
        "system_user"
    )
    primary_settings["password"] = top_settings["integrations.ad"][index].get(
        "password"
    )
    primary_settings["properties"] = top_settings["integrations.ad"][index].get(
        "properties"
    )

    missing = []
    for key, val in primary_settings.items():
        if val is None:
            missing.append(key)

    # 36182 exclude non primary AD-users
    primary_settings["discriminator.field"] = top_settings["integrations.ad"][
        index
    ].get("discriminator.field")
    if primary_settings["discriminator.field"] is not None:

        # if we have a field we MUST have .values and .function
        primary_settings["discriminator.values"] = top_settings["integrations.ad"][
            index
        ].get("discriminator.values")
        if primary_settings["discriminator.values"] is None:
            missing.append("discriminator.values")

        primary_settings["discriminator.function"] = top_settings["integrations.ad"][
            index
        ].get("discriminator.function")
        if primary_settings["discriminator.function"] is None:
            missing.append("discriminator.function")

        if not primary_settings["discriminator.function"] in ["include", "exclude"]:
            raise ValueError(
                "'ad.discriminator.function'"
                + " must be 'include' or 'exclude' for AD %d" % index
            )

    # Settings that do not need to be set, or have defaults
    index_settings = top_settings["integrations.ad"][index]
    primary_settings["servers"] = index_settings.get("servers", [])
    primary_settings["caseless_samname"] = index_settings.get("caseless_samname", True)
    primary_settings["sam_filter"] = index_settings.get("sam_filter", "")
    primary_settings["cpr_separator"] = index_settings.get("cpr_separator", "")
    primary_settings["pseudo_cprs"] = index_settings.get("pseudo_cprs", [])

    primary_settings["method"] = index_settings.get("method", "kerberos")

    primary_settings["ad_mo_sync_mapping"] = index_settings.get(
        "ad_mo_sync_mapping", {}
    )
    primary_settings["ad_mo_sync_terminate_missing"] = index_settings.get(
        "ad_mo_sync_terminate_missing", False
    )
    primary_settings[
        "ad_mo_sync_terminate_missing_require_itsystem"
    ] = index_settings.get("ad_mo_sync_terminate_missing_require_itsystem", True)
    primary_settings["ad_mo_sync_terminate_disabled"] = index_settings.get(
        "ad_mo_sync_terminate_disabled"
    )
    primary_settings["ad_mo_sync_pre_filters"] = index_settings.get(
        "ad_mo_sync_pre_filters", []
    )
    primary_settings["ad_mo_sync_terminate_disabled_filters"] = index_settings.get(
        "ad_mo_sync_terminate_disabled_filters", []
    )

    if missing:
        msg = "Missing settings in AD {}: {}".format(index, missing)
        logger.error(msg)
        raise Exception(msg)

    return primary_settings


def _read_primary_write_information(top_settings):
    """
    Read the configuration for writing to the primary AD. If anything is missing,
    the AD write will be disabled.
    """
    # TODO: Some happy day, we could check for the actual validity of these

    # Straight-forward field mappings
    required_keys = {
        "uuid_field",
        "level2orgunit_field",
        "level2orgunit_type",
        "upn_end",
    }
    conf = {
        key: top_settings["integrations.ad.write.%s" % key] for key in required_keys
    }

    # Special field mappings
    conf["cpr_field"] = top_settings["integrations.ad"][0]["cpr_field"]
    conf["org_field"] = top_settings["integrations.ad.write.org_unit_field"]

    missing = {key for key, val in conf.items() if val is None}
    if missing:
        msg = "Missing values for AD write: %r"
        logger.info(msg, missing)
        raise ValueError(msg % missing)

    # Template fields
    conf["mo_to_ad_fields"] = top_settings.get(
        "integrations.ad_writer.mo_to_ad_fields", {}
    )
    conf["template_to_ad_fields"] = top_settings.get(
        "integrations.ad_writer.template_to_ad_fields", {}
    )

    # Overrides the "-Path" argument to "New-ADUser", if set
    if "integrations.ad_writer.new_ad_user_path" in top_settings:
        conf["new_ad_user_path"] = top_settings[
            "integrations.ad_writer.new_ad_user_path"
        ]

    # Check for illegal configuration of AD Write.
    mo_to_ad_fields = conf["mo_to_ad_fields"]
    template_to_ad_fields = conf["template_to_ad_fields"]
    other_ad_field_names = [conf["org_field"], conf["uuid_field"]]
    if conf.get("level2orgunit_field"):
        other_ad_field_names.append(conf["level2orgunit_field"])
    ad_field_names = (
        list(mo_to_ad_fields.values())
        + list(template_to_ad_fields.keys())
        + other_ad_field_names
    )
    # Conflicts are case-insensitive
    counter = collections.Counter(map(str.lower, ad_field_names))
    dupes = sorted(set(name for name, count in counter.items() if count > 1))
    if dupes:
        msg = "Duplicated AD field names in settings: %r"
        logger.info(msg, dupes)
        raise ValueError(msg % dupes)

    # Check that all settings we write to are in properties for all ADs
    for ad_settings in top_settings["integrations.ad"]:
        properties = set(map(str.lower, ad_settings.get("properties", [])))
        missing_properties = list(
            filter(
                lambda ad_field: ad_field != "" and ad_field.lower() not in properties,
                ad_field_names,
            )
        )
        if missing_properties:
            msg = "Missing AD field names in properties: %r"
            logger.info(msg, missing_properties)
            raise ValueError(msg % missing_properties)

    return conf


def read_settings(top_settings=None, index=0):
    if top_settings is None:
        top_settings = load_settings()

    settings = {}
    settings["global"] = _read_global_settings(top_settings)
    settings["primary"] = _read_primary_ad_settings(top_settings, index)
    # TODO: better check for AD-writer.
    if "integrations.ad.write.level2orgunit_field" in top_settings:
        settings["primary_write"] = _read_primary_write_information(top_settings)
    return settings


def injected_settings(
    settings_key: str,
    ad_settings: Optional[Dict] = None,
    normal_settings: Optional[Dict] = None,
) -> Dict:
    ad_settings = ad_settings or read_settings()
    normal_settings = normal_settings or load_settings()
    inject = normal_settings.get(settings_key, {})
    for path, value in inject.items():
        assign(ad_settings, path, value)
    return ad_settings


@click.command()
@click.option("--inject", is_flag=True, help="inject AD_life_cycle settings")
def show_ad_settings(inject):
    settings = (
        injected_settings("ad_lifecycle_injected_settings")
        if inject
        else read_settings()
    )
    click.echo(settings)


if __name__ == "__main__":
    show_ad_settings()
