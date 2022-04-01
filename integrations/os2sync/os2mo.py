#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from uuid import UUID
from operator import itemgetter
import requests
from more_itertools import first
from more_itertools import one


from constants import AD_it_system
from exporters.utils.priority_by_class import choose_public_address
from integrations.os2sync.config import get_os2sync_settings
from integrations.os2sync import config
from integrations.os2sync.templates import Person
from integrations.os2sync.templates import User
from ra_utils.headers import TokenSettings

logger = logging.getLogger(config.loggername)


@lru_cache
def get_mo_session():
    session = requests.Session()
    session.verify = get_os2sync_settings().os2sync_ca_verify_os2mo
    session.headers = {
        "User-Agent": "os2mo-data-import-and-export",
    }
    session_headers = TokenSettings().get_headers()
    if session_headers:
        session.headers.update(session_headers)
    return session




# Actually recursive, but couple of levels here
JSON_TYPE = Dict[str, Union[None, str, Dict[str, Union[None, str, Dict[str, Any]]]]]


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
        return [
            cls(
                system_name=it_obj["itsystem"]["name"].strip(),
                user_key=it_obj["user_key"].strip(),
            )
            for it_obj in response
        ]

    def __repr__(self):
        return "{}(system_name={},user_key={})".format(
            self.__class__.__name__, self.system_name, self.user_key
        )

    def __eq__(self, other):
        if not isinstance(other, IT):
            raise TypeError("unexpected type: {}".format(other))

        return self.system_name == other.system_name and self.user_key == other.user_key


# truncate and warn all strings in dictionary,
# ensure not shortening uuids
def strip_truncate_and_warn(d, root, length):
    for k, v in list(d.items()):
        if isinstance(v, dict):
            strip_truncate_and_warn(v, root, length)
        elif isinstance(v, str):
            v = d[k] = v.strip()
            if len(v) > length:
                v = d[k] = v[:length]
                logger.warning(
                    "truncating to %d key '%s' for" " uuid '%s' to value '%s'",
                    length,
                    k,
                    root["Uuid"],
                    v,
                )


@lru_cache
def os2mo_get(url, **params):
    #format url like {BASE}/service
    mora_base = get_os2sync_settings().mora_base
    
    url = url.format(BASE=f"{mora_base}/service")
    try:
        session = get_mo_session()
        r = session.get(url, params=params)
        r.raise_for_status()
        return r
    except Exception:
        logger.exception(url)
        raise


def has_kle():
    os2mo_config = os2mo_get("{BASE}/configuration").json()
    return os2mo_config["show_kle"]



def addresses_to_user(user, addresses, phone_scope_classes, email_scope_classes):
    # TODO: This looks like bucketing (more_itertools.bucket)
    emails, phones = [], []
    for address in addresses:
        if address["address_type"]["scope"] == "EMAIL":
            emails.append(address)
        if address["address_type"]["scope"] == "PHONE":
            phones.append(address)

    # find phone using prioritized/empty list of address_type uuids
    phone = choose_public_address(phones, phone_scope_classes)
    if phone:
        user["PhoneNumber"] = phone["name"]

    # find email using prioritized/empty list of address_type uuids
    email = choose_public_address(emails,email_scope_classes)
    if email:
        user["Email"] = email["name"]


def engagements_to_user(user, engagements, allowed_unitids):
    for e in sorted(engagements, key=lambda e: e["job_function"]["name"] + e["uuid"]):
        if e["org_unit"]["uuid"] in allowed_unitids:
            user["Positions"].append(
                {
                    "OrgUnitUuid": e["org_unit"]["uuid"],
                    "Name": e["job_function"]["name"],
                    #Only used to find primary engamgements work-address
                    "is_primary": e["is_primary"],
                }
            )


def try_get_ad_user_key(uuid: str) -> Optional[str]:
    """
    fetches all it-systems related to a user and return the ad-user_key if exists
    """
    it_response = os2mo_get("{BASE}/e/" + uuid + "/details/it").json()
    it_systems = IT.from_mo_json(it_response)
    ad_systems = list(filter(lambda x: x.system_name == AD_it_system, it_systems))

    # if no ad OR multiple
    if len(ad_systems) != 1:
        return
    return ad_systems[0].user_key

def get_work_address(positions, work_address_names):
    #find the primary engagement and lookup the addresses for that unit
    primary = filter(lambda e: e["is_primary"], positions)
    try:
        primary_eng = one(primary)
    except ValueError:
        logger.error("Could not get unique primary engagement, using first found position")
        primary_eng = first(positions)

    org_addresses = os2mo_get(
        "{BASE}/ou/" + primary_eng["OrgUnitUuid"] + "/details/address"
    ).json()
    #filter and sort based on settings and use the first match if any
    work_address = filter(lambda addr: addr["address_type"]["name"] in work_address_names , org_addresses)
    work_address = sorted(work_address, key=lambda a: work_address_names.index(a["address_type"]["name"]))
    chosen_work_address= first(work_address, default={})
    return chosen_work_address.get("name")

def get_sts_user(uuid, allowed_unitids, settings):
    employee = os2mo_get("{BASE}/e/" + uuid + "/").json()
    
    user = User(
        dict(
            uuid=uuid,
            candidate_user_id=try_get_ad_user_key(uuid),
            person=Person(employee, settings=settings),
        ),
        settings=settings,
    )

    sts_user = user.to_json()

    addresses_to_user(
        sts_user, os2mo_get("{BASE}/e/" + uuid + "/details/address").json(), phone_scope_classes=settings.os2sync_phone_scope_classes, email_scope_classes=settings.os2sync_email_scope_classes
    )
    #use calculate_primary flag to get the is_primary boolean used in getting work-address
    engagements = os2mo_get(
            "{BASE}/e/" + uuid + "/details/engagement?calculate_primary=true"
        ).json()
    engagements_to_user(
        sts_user,
        engagements,
        allowed_unitids,
    )

    # Optionally find the work address of employees primary engagement.
    work_address_names = settings.os2sync_employee_engagement_address
    if sts_user["Positions"] and work_address_names:
        sts_user["Location"] = get_work_address(sts_user["Positions"], work_address_names)
    truncate_length = max(36, settings.os2sync_truncate_length)
    strip_truncate_and_warn(sts_user, sts_user, length=truncate_length)
    return sts_user


def org_unit_uuids(**kwargs):
    org_uuid = one(os2mo_get("{BASE}/o/").json())["uuid"]
    return [
        ou["uuid"]
        for ou in os2mo_get(f"{{BASE}}/o/{org_uuid}/ou/", limit=999999, **kwargs).json()[
            "items"
        ]
    ]


def manager_to_orgunit(unit_uuid: UUID) -> UUID:
    manager = os2mo_get("{BASE}/ou/" + str(unit_uuid) + "/details/manager").json()
    if manager:
        return UUID(one(manager)["person"]["uuid"])


def itsystems_to_orgunit(orgunit, itsystems):
    for i in itsystems:
        orgunit["ItSystemUuids"].append(i["itsystem"]["uuid"])


def address_type_is(address, user_key=None, scope="TEXT"):
    return (
        address["address_type"]["user_key"] == user_key
        and address["address_type"]["scope"] == scope
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


def filter_kle(aspect: str, kle) -> List[UUID]:
    """Filters kle by aspect name

    KLE aspects can be "Udførende", "Ansvarlig" or "Indsigt"

    Returns:
        list of uuids
    """
    tasks_kle = filter(lambda k: one(k["kle_aspect"])["name"] == aspect, kle)
    task_uuids = set(k["kle_number"]["uuid"] for k in tasks_kle)
    return list(sorted(task_uuids))


def partition_kle(kle, use_contact_for_tasks) -> (List[UUID], List[UUID]):
    """Collect kle uuids according to kle_aspect.

    Default is to return all KLE uuids as Tasks,
    If the setting 'os2sync_use_contact_for_tasks' is set KLEs will be divided:

    * Aspect "Udførende" goes into "Tasks"
    * Aspect "Ansvarlig" goes into "ContactForTasks"

    Args:
        kle: A list of KLEs.

    Returns:
        Tuple(List, List)
    """

    if use_contact_for_tasks:
        tasks = filter_kle("Udførende", kle)
        ContactForTasks = filter_kle("Ansvarlig", kle)

        return tasks, ContactForTasks

    tasks = set()

    for k in kle:
        uuid = k["kle_number"]["uuid"]
        tasks.add(uuid)

    return list(sorted(tasks)), []


def kle_to_orgunit(org_unit: Dict, kle: Dict, use_contact_for_tasks):
    """Mutates the dict "org_unit" to include KLE data"""
    tasks, contactfortasks = partition_kle(kle, use_contact_for_tasks=use_contact_for_tasks)
    if tasks:
        org_unit["Tasks"] = tasks
    if contactfortasks:
        org_unit["ContactForTasks"] = contactfortasks


def is_ignored(unit, settings):
    """Determine if unit should be left out of transfer

    Args:
        unit: The organization unit to enrich with kle information.
        settings: a dictionary

    Returns:
        Boolean
    """

    return (
        unit.get("org_unit_level")
        and UUID(unit["org_unit_level"]["uuid"]) in settings.os2sync_ignored_unit_levels
    ) or (
        unit.get("org_unit_type")
        and UUID(unit["org_unit_type"]["uuid"]) in settings.os2sync_ignored_unit_types
    )


def get_sts_orgunit(uuid, settings):
    base = parent = os2mo_get("{BASE}/ou/" + uuid + "/").json()

    if is_ignored(base, settings):
        logger.info("Ignoring %r", base)
        return None
    top_unit_uuid = str(settings.os2sync_top_unit_uuid)
    if not parent["uuid"] == top_unit_uuid:
        while parent.get("parent"):
            if parent["uuid"] == top_unit_uuid:
                break
            parent = parent["parent"]

    if not parent["uuid"] == top_unit_uuid:
        # not part of right tree
        logger.debug(f"ignoring unit {uuid=}, as it is not a unit bellow {top_unit_uuid=}")
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

    if settings.os2sync_sync_managers:
        manager_uuid = manager_to_orgunit(uuid)
        if manager_uuid:
            sts_org_unit["managerUuid"] = str(manager_uuid)

    # this is set by __main__
    if has_kle():
        kle_to_orgunit(
            sts_org_unit,
            os2mo_get("{BASE}/ou/" + uuid + "/details/kle").json(),
            use_contact_for_tasks=settings.os2sync_use_contact_for_tasks
        )

    # show_all_details(uuid,"ou")
    strip_truncate_and_warn(sts_org_unit, sts_org_unit, settings.os2sync_truncate_length)
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
                os2mo_get("{BASE}/" + objtyp + "/" + uuid + "/details/" + d).json()
            )
    print(" ---- end of details ----\n")
