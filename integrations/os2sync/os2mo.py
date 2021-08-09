#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
from typing import Any, Dict, List, Optional, Union

import requests
from functools import lru_cache
from constants import AD_it_system, FK_ORG_it_system
from exporters.utils.priority_by_class import choose_public_address
from integrations.os2sync import config
from integrations.os2sync.templates import Person, User

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

# Actually recursive, but couple of levels here
JSON_TYPE = Dict[str, Union[None, str,
                            Dict[str, Union[None, str,
                                            Dict[str, Any]]]]]


class IT:
    def __init__(self, system_name: str, user_key: str):
        self.system_name = system_name
        self.user_key = user_key

    @classmethod
    def from_mo_json(cls, response: List[JSON_TYPE]):
        """
        Designed to parse the response from MO when requesting a users it-systems
        Concretely, expects a list of this style of object:

        {'itsystem': {'name': 'Active Directory',
              'reference': None,
              'system_type': None,
              'user_key': 'Active Directory',
              'uuid': 'a1608e69-c422-404f-a6cc-b873c50af111',
              'validity': {'from': '1900-01-01', 'to': None}},
         'org_unit': None,
         'person': {'givenname': 'Solveig',
                    'name': 'Solveig Kuhlenhenke',
                    'nickname': '',
                    'nickname_givenname': '',
                    'nickname_surname': '',
                    'surname': 'Kuhlenhenke',
                    'uuid': '23d2dfc7-6ceb-47cf-97ed-db6beadcb09b'},
         'user_key': 'SolveigK',
         'uuid': 'a2fb2581-c57a-46ad-8a21-30118a3859b7',
         'validity': {'from': '2003-08-13', 'to': None}}
        """
        return [cls(system_name=it_obj['itsystem']['name'].strip(),
                    user_key=it_obj['user_key'].strip())
                for it_obj in response]

    def __repr__(self):
        return '{}(system_name={},user_key={})'.format(self.__class__.__name__,
                                                       self.system_name, self.user_key)

    def __eq__(self, other):
        if not isinstance(other, IT):
            raise TypeError('unexpected type: {}'.format(other))

        return self.system_name == other.system_name and self.user_key == other.user_key


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
        os2mo_get("{BASE}/o/{ORG}/f/kle_aspect/")
        os2mo_get("{BASE}/o/{ORG}/f/kle_number/")
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


def addresses_to_user(user, addresses):
    # TODO: This looks like bucketing (more_itertools.bucket)
    emails, phones = [], []
    for address in addresses:
        if address["address_type"]["scope"] == "EMAIL":
            emails.append(address)
        if address["address_type"]["scope"] == "PHONE":
            phones.append(address)
        if address["address_type"]["scope"] == "DAR":
            user["Location"] = address["name"]

    # find phone using prioritized/empty list of address_type uuids
    phone = choose_public_address(phones, settings["OS2SYNC_PHONE_SCOPE_CLASSES"])
    if phone:
        user["PhoneNumber"] = phone["name"]

    # find email using prioritized/empty list of address_type uuids
    email = choose_public_address(emails, settings["OS2SYNC_EMAIL_SCOPE_CLASSES"])
    if email:
        user["Email"] = email["name"]


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
@lru_cache
def read_user_it(uuid):
    it_response = os2mo_get("{BASE}/e/" + uuid + "/details/it")
    it_response.raise_for_status()
    return it_response.json()
    
def get_user_it_account(uuid: str, it_system_name: str):
    """
    fetches all it-systems related to a user and return the user_key if exists
    """
    it_response = read_user_it(uuid)
    it_systems = IT.from_mo_json(it_response)
    ad_systems = list(filter(lambda x: x.system_name == it_system_name,
                             it_systems))

    # if no ad OR multiple
    if len(ad_systems) != 1:
        return
    return ad_systems[0].user_key

def try_get_ad_user_key(uuid: str) -> Optional[str]:
    return get_user_it_account(uuid, AD_it_system)
    
def try_get_fk_org_uuid(uuid:str) -> str:
    return get_user_it_account(uuid, FK_ORG_it_system) or uuid

def get_sts_user(uuid, allowed_unitids):
    employee = os2mo_get("{BASE}/e/" + uuid + "/").json()

    user = User(
        dict(
            uuid=try_get_fk_org_uuid(uuid),
            candidate_user_id=try_get_ad_user_key(uuid),
            person=Person(employee, settings=settings),
        ),
        settings=settings,
    )

    sts_user = user.to_json()

    addresses_to_user(
        sts_user, os2mo_get("{BASE}/e/" + uuid + "/details/address").json()
    )

    engagements_to_user(
        sts_user,
        os2mo_get("{BASE}/e/" + uuid + "/details/engagement").json(),
        allowed_unitids
    )

    strip_truncate_and_warn(sts_user, sts_user)
    return sts_user


def org_unit_uuids(**kwargs):
    return [
        ou["uuid"]
        for ou in os2mo_get("{BASE}/o/{ORG}/ou/", limit=999999,
                            **kwargs).json()["items"]
    ]


def itsystems_to_orgunit(orgunit, itsystems):
    for i in itsystems:
        orgunit["ItSystemUuids"].append(i["itsystem"]["uuid"])


def address_type_is(address, user_key=None, scope="TEXT"):
    return (
        address["address_type"]["user_key"] == user_key and
        address["address_type"]["scope"] == scope
    )


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
        elif address_type_is(a, user_key="ContactOpenHours"):
            orgunit["ContactOpenHours"] = a["name"]
        elif address_type_is(a, user_key="DtrId"):
            orgunit["DtrId"] = a["name"]


def kle_to_orgunit(orgunit, kle):
    """Collect kle uuids according to kle_aspect.

    * Aspect "Udførende" goes into "Tasks"
    * Aspect "Ansvarlig" goes into "ContactForTasks"

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
