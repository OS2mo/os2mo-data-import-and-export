#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging

import requests

from integrations.os2sync import config

settings = config.settings
logger = logging.getLogger(config.loggername)

session = requests.Session()
session.verify = settings["OS2MO_CA_BUNDLE"]
session.headers = {
    "User-Agent": "os2mo-data-import-and-export",
}

if settings["OS2MO_SAML_TOKEN"] is not None:
    session.headers["SESSION"] = settings["OS2MO_SAML_TOKEN"]


TRUNCATE_LENGTH = max(36, int(settings.get("OS2SYNC_TRUNCATE", 200)))


# truncate and warn all strings in dictionary,
# ensure not shortening uuids
def strip_truncate_and_warn(d, root, length=TRUNCATE_LENGTH):
    for k, v in list(d.items()):
        if isinstance(v, dict):
            strip_truncate_and_warn(v, root)
        elif isinstance(v, str):
            v = d[k] = v.strip()
            if len(v) > length:
                v = d[k] = v[:length]
                logger.warning(
                    "truncating to %d key '%s' for"
                    " uuid '%s' to value '%s'",
                    length,
                    k,
                    root["Uuid"],
                    v
                )


def os2mo_url(url):
    """format url like {BASE}/o/{ORG}/e
    """
    url = url.format(
        BASE=settings["OS2MO_SERVICE_URL"], ORG=settings["OS2MO_ORG_UUID"]
    )
    return url


def os2mo_get(url, **params):
    url = os2mo_url(url)
    try:
        r = session.get(url, params=params)
        r.raise_for_status()
        return r
    except Exception:
        logger.exception(url)
        raise


def has_kle():
    try:
        os2mo_get("{BASE}/o/{ORG}/f/kle_aspect")
        os2mo_get("{BASE}/o/{ORG}/f/kle_number")
        os2mo_get(
            "{BASE}/ou/" +
            settings["OS2MO_TOP_UNIT_UUID"] +
            "/details/kle"
        )
        return True
    except requests.exceptions.HTTPError:
        return False


def user_uuids(**kwargs):
    return [
        e["uuid"]
        for e in os2mo_get("{BASE}/o/{ORG}/e/", limit=9999999,
                           **kwargs).json()["items"]
    ]


def chose_visible_prioritized_address(candidates, prioritized_classes):
    chosen = None
    # find candidate using prioritized list if available
    for cls in prioritized_classes:
        if chosen:
            break
        for candidate in candidates:
            if (
                candidate["address_type"]["uuid"] == cls
                and candidate.get("visibility",
                                  {"scope": "PUBLIC"})["scope"] == "PUBLIC"
            ):
                chosen = {"Value": candidate["name"],
                          "Uuid": candidate["uuid"]}

    if not prioritized_classes and len(candidates):
        for candidate in reversed(candidates):
            if candidate.get("visibility",
                             {"scope": "PUBLIC"})["scope"] == "PUBLIC":
                chosen = {"Value": candidate["name"],
                          "Uuid": candidate["uuid"]}
                break

    return chosen


def addresses_to_user(user, addresses):
    emails, phones = [], []
    for a in addresses:
        if a["address_type"]["scope"] == "EMAIL":
            emails.append(a)
        if a["address_type"]["scope"] == "PHONE":
            phones.append(a)
        if a["address_type"]["scope"] == "DAR":
            user["Location"] = a["name"]

    phones = sorted(phones, key=lambda phone: phone["uuid"])
    emails = sorted(emails, key=lambda email: email["uuid"])

    # find phone using prioritized/empty list of address_type uuids
    phone = chose_visible_prioritized_address(
        phones,
        settings["OS2SYNC_PHONE_SCOPE_CLASSES"]
    )
    if phone:
        user["PhoneNumber"] = phone["Value"]

    # find email using prioritized/empty list of address_type uuids
    email = chose_visible_prioritized_address(
        emails,
        settings["OS2SYNC_EMAIL_SCOPE_CLASSES"]
    )
    if email:
        user["Email"] = email["Value"]


def engagements_to_user(user, engagements, allowed_unitids):
    for e in sorted(engagements,
                    key=lambda e: e["job_function"]["name"] + e["uuid"]):
        if e["org_unit"]["uuid"] in allowed_unitids:
            user["Positions"].append(
                {
                    "OrgUnitUuid": e["org_unit"]["uuid"],
                    "Name": e["job_function"]["name"],
                }
            )


def get_sts_user(uuid, allowed_unitids):
    base = os2mo_get("{BASE}/e/" + uuid + "/").json()
    sts_user = {
        "Uuid": uuid,
        "UserId": uuid,
        "Positions": [],
        "Person": {"Name": base["name"], "Cpr": base["cpr_no"]},
    }
    if not settings["OS2SYNC_XFER_CPR"]:
        sts_user["Person"]["Cpr"] = None

    addresses_to_user(
        sts_user, os2mo_get("{BASE}/e/" + uuid + "/details/address").json()
    )
    engagements_to_user(
        sts_user,
        os2mo_get("{BASE}/e/" + uuid + "/details/engagement").json(),
        allowed_unitids
    )
    # show_all_details(uuid,"e")
    strip_truncate_and_warn(sts_user, sts_user)
    return sts_user


def org_unit_uuids(**kwargs):
    return [
        ou["uuid"]
        for ou in os2mo_get("{BASE}/o/{ORG}/ou", limit=999999,
                            **kwargs).json()["items"]
    ]


def itsystems_to_orgunit(orgunit, itsystems):
    for i in itsystems:
        orgunit["ItSystemUuids"].append(i["itsystem"]["uuid"])


def addresses_to_orgunit(orgunit, addresses):
    for a in addresses:
        if a["address_type"]["scope"] == "EMAIL":
            orgunit["Email"] = a["name"]
        elif a["address_type"]["scope"] == "EAN":
            orgunit["Ean"] = a["name"]
        elif a["address_type"]["scope"] == "PHONE":
            orgunit["PhoneNumber"] = a["name"]
        elif a["address_type"]["scope"] == "DAR":
            orgunit["Post"] = a["name"]
        elif a["address_type"]["scope"] == "PNUMBER":
            orgunit["Location"] = a["name"]



def kle_to_orgunit(orgunit, kle):
    """Collect kle uuids according to kle_aspect.

    * Aspect "Udførende" goes into "Tasks"
    * Aspect "Ansvarlig" goes into "ContactForTasks"

    Example:

        >>> orgunit={}
        >>> kles = [
        ...     {'uuid': 1, 'kle_number': {'uuid': '3'}},
        ...     {'uuid': 2, 'kle_number': {'uuid': '4'}},
        ... ]
        >>> kle_to_orgunit(orgunit, kles)
        >>> orgunit
        {'Tasks': ['3', '4']}


    Args:
        orgunit: The organization unit to enrich with kle information.
        kle: A list of KLEs.

    Returns:
        None
    """
    tasks = set()

    for k in kle:
        uuid = k["kle_number"]["uuid"]
        tasks.add(uuid)

    if len(tasks):
        orgunit["Tasks"] = list(sorted(tasks))


def is_ignored(unit, settings):
    """Determine if unit should be left out of transfer

    Example:
        >>> settings={
        ... "OS2SYNC_IGNORED_UNIT_LEVELS": ["10","2"],
        ... "OS2SYNC_IGNORED_UNIT_TYPES":['6','7']}
        >>> unit={"org_unit_level":{"uuid":"1"}, "org_unit_type":{"uuid":"5"}}
        >>> is_ignored(unit, settings)
        False
        >>> unit={"org_unit_level":{"uuid":"2"}, "org_unit_type":{"uuid":"5"}}
        >>> is_ignored(unit, settings)
        True
        >>> unit={"org_unit_level":{"uuid":"1"}, "org_unit_type":{"uuid":"6"}}
        >>> is_ignored(unit, settings)
        True

    Args:
        unit: The organization unit to enrich with kle information.
        settings: a dictionary

    Returns:
        Boolean
    """

    return (
        unit.get("org_unit_level") and unit["org_unit_level"]["uuid"] in settings[
            "OS2SYNC_IGNORED_UNIT_LEVELS"
        ]
    ) or (
        unit.get("org_unit_type") and unit["org_unit_type"]["uuid"] in settings[
            "OS2SYNC_IGNORED_UNIT_TYPES"
        ]
    )


def get_sts_orgunit(uuid):
    base = parent = os2mo_get("{BASE}/ou/" + uuid + "/").json()

    if is_ignored(base, settings):
        logger.info("Ignoring %r", base)
        return None

    if not parent["uuid"] == settings["OS2MO_TOP_UNIT_UUID"]:
        while parent.get("parent"):
            if parent["uuid"] == settings["OS2MO_TOP_UNIT_UUID"]:
                break
            parent = parent["parent"]

    if not parent["uuid"] == settings["OS2MO_TOP_UNIT_UUID"]:
        # not part of right tree
        return None

    sts_org_unit = {"ItSystemUuids": [], "Name": base["name"], "Uuid": uuid}

    if base.get("parent") and "uuid" in base["parent"]:
        sts_org_unit["ParentOrgUnitUuid"] = base["parent"]["uuid"]

    itsystems_to_orgunit(
        sts_org_unit, os2mo_get("{BASE}/ou/" + uuid + "/details/it").json()
    )
    addresses_to_orgunit(
        sts_org_unit,
        os2mo_get("{BASE}/ou/" + uuid + "/details/address").json(),
    )
    # this is set by __main__
    if settings["OS2MO_HAS_KLE"]:
        kle_to_orgunit(
            sts_org_unit,
            os2mo_get("{BASE}/ou/" + uuid + "/details/kle").json(),
        )

    # show_all_details(uuid,"ou")
    strip_truncate_and_warn(sts_org_unit, sts_org_unit)
    return sts_org_unit


def show_all_details(uuid, objtyp):
    import pprint

    print(" ---- details ----\n")
    for d, has_detail in (
        os2mo_get("{BASE}/" + objtyp + "/" + uuid + "/details").json().items()
    ):
        if has_detail:
            print("------------ detail ---- " + d)
            pprint.pprint(
                os2mo_get(
                    "{BASE}/" + objtyp + "/" + uuid + "/details/" + d
                ).json()
            )
    print(" ---- end of details ----\n")
