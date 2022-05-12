#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from functools import lru_cache
from operator import itemgetter
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from uuid import UUID

import requests
from more_itertools import first
from more_itertools import one
from ra_utils.headers import TokenSettings

from exporters.utils.priority_by_class import choose_public_address
from integrations.os2sync import config
from integrations.os2sync.config import get_os2sync_settings
from integrations.os2sync.templates import Person
from integrations.os2sync.templates import User

logger = logging.getLogger(config.loggername)


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


class IT:
    def __init__(self, system_name: str, user_key: str):
        self.system_name = system_name
        self.user_key = user_key

    @classmethod
    def from_mo_json(cls, response: List):
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
    # format url like {BASE}/service
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
    # `phone_scope_classes` and `email_scope_classes` are both lists of UUIDs.
    # We need to convert them to lists of strings in order to make them work correctly
    # with `choose_public_address`.
    phone_scope_classes = [str(cls) for cls in phone_scope_classes]
    email_scope_classes = [str(cls) for cls in email_scope_classes]

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
    email = choose_public_address(emails, email_scope_classes)
    if email:
        user["Email"] = email["name"]


def engagements_to_user(user, engagements, allowed_unitids):
    for e in sorted(engagements, key=lambda e: e["job_function"]["name"] + e["uuid"]):
        if e["org_unit"]["uuid"] in allowed_unitids:
            user["Positions"].append(
                {
                    "OrgUnitUuid": e["org_unit"]["uuid"],
                    "Name": e["job_function"]["name"],
                    # Only used to find primary engagements work-address
                    "is_primary": e["is_primary"],
                }
            )


def try_get_ad_user_key(uuid: str, user_key_it_system_name) -> Optional[str]:
    """
    fetches all it-systems related to a user and return the ad-user_key if exists
    """
    it_response = os2mo_get("{BASE}/e/" + uuid + "/details/it").json()
    it_systems = IT.from_mo_json(it_response)
    ad_systems = list(
        filter(lambda x: x.system_name == user_key_it_system_name, it_systems)
    )

    # if no ad OR multiple
    if len(ad_systems) != 1:
        return None
    return ad_systems[0].user_key


def get_work_address(positions, work_address_names) -> Optional[str]:
    # find the primary engagement and lookup the addresses for that unit
    primary = filter(lambda e: e["is_primary"], positions)
    try:
        primary_eng = one(primary)
    except ValueError:
        logger.error(
            "Could not get unique primary engagement, using first found position"
        )
        primary_eng = first(positions)

    org_addresses = os2mo_get(
        "{BASE}/ou/" + primary_eng["OrgUnitUuid"] + "/details/address"
    ).json()
    # filter and sort based on settings and use the first match if any
    work_address: List[Dict] = list(
        filter(
            lambda addr: addr["address_type"]["name"] in work_address_names,
            org_addresses,
        )
    )
    work_address = list(
        sorted(
            work_address,
            key=lambda a: work_address_names.index(a["address_type"]["name"]),
        )
    )
    chosen_work_address: Dict = first(work_address, default={})
    return chosen_work_address.get("name")


def get_fk_org_uuid(
    it_accounts: Dict, mo_uuid: str, uuid_from_it_systems: List[str]
) -> str:
    """Find FK-org uuid from it-accounts based on the given list of it-system names."""
    it = list(
        filter(lambda i: i["itsystem"]["name"] in uuid_from_it_systems, it_accounts)
    )
    # Sort the relevant it-systems based on their position in the given list
    it.sort(key=lambda name: uuid_from_it_systems.index(name["itsystem"]["name"]))
    it = list(map(itemgetter("uuid"), it))
    # Append mo_uuid to return it if no matches were found in it-accounts
    it.append(mo_uuid)
    return first(it)


def get_sts_user(uuid, settings):
    employee = os2mo_get("{BASE}/e/" + uuid + "/").json()

    user = User(
        dict(
            uuid=uuid,
            candidate_user_id=try_get_ad_user_key(
                uuid, user_key_it_system_name=settings.os2sync_user_key_it_system_name
            ),
            person=Person(employee, settings=settings),
        ),
        settings=settings,
    )

    sts_user = user.to_json()

    addresses_to_user(
        sts_user,
        os2mo_get("{BASE}/e/" + uuid + "/details/address").json(),
        phone_scope_classes=settings.os2sync_phone_scope_classes,
        email_scope_classes=settings.os2sync_email_scope_classes,
    )
    # use calculate_primary flag to get the is_primary boolean used in getting work-address
    engagements = os2mo_get(
        "{BASE}/e/" + uuid + "/details/engagement?calculate_primary=true"
    ).json()
    allowed_unitids = org_unit_uuids(root=settings.os2sync_top_unit_uuid)
    engagements_to_user(sts_user, engagements, allowed_unitids)

    # Optionally find the work address of employees primary engagement.
    work_address_names = settings.os2sync_employee_engagement_address
    if sts_user["Positions"] and work_address_names:
        sts_user["Location"] = get_work_address(
            sts_user["Positions"], work_address_names
        )
    truncate_length = max(36, settings.os2sync_truncate_length)
    strip_truncate_and_warn(sts_user, sts_user, length=truncate_length)

    if settings.os2sync_uuid_from_it_systems:
        it = os2mo_get(f"{{BASE}}/e/{uuid}/details/it").json()
        sts_user["Uuid"] = get_fk_org_uuid(
            it, uuid, settings.os2sync_uuid_from_it_systems
        )
    return sts_user


@lru_cache()
def organization_uuid() -> str:
    return one(os2mo_get("{BASE}/o/").json())["uuid"]


@lru_cache()
def org_unit_uuids(**kwargs) -> Set[str]:
    org_uuid = organization_uuid()
    ous = os2mo_get(f"{{BASE}}/o/{org_uuid}/ou/", limit=999999, **kwargs).json()[
        "items"
    ]
    return set(map(itemgetter("uuid"), ous))


def manager_to_orgunit(unit_uuid: str) -> Optional[str]:
    manager = os2mo_get("{BASE}/ou/" + unit_uuid + "/details/manager").json()
    if not manager:
        return None
    return one(manager)["person"]["uuid"]


def itsystems_to_orgunit(orgunit, itsystems, uuid_from_it_systems):
    itsystems = filter(
        lambda i: i["itsystem"]["name"] not in uuid_from_it_systems, itsystems
    )
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


def filter_kle(aspect: str, kle) -> List[str]:
    """Filters kle by aspect name

    KLE aspects can be "Udførende", "Ansvarlig" or "Indsigt"

    Returns:
        list of uuids
    """
    tasks_kle = filter(lambda k: one(k["kle_aspect"])["name"] == aspect, kle)
    task_uuids = set(k["kle_number"]["uuid"] for k in tasks_kle)
    return list(sorted(task_uuids))


def partition_kle(kle, use_contact_for_tasks) -> Tuple[List[str], List[str]]:
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

    tasks_set = set()

    for k in kle:
        uuid = k["kle_number"]["uuid"]
        tasks_set.add(uuid)

    return list(sorted(tasks_set)), []


def kle_to_orgunit(org_unit: Dict, kle: Dict, use_contact_for_tasks):
    """Mutates the dict "org_unit" to include KLE data"""
    tasks, contactfortasks = partition_kle(
        kle, use_contact_for_tasks=use_contact_for_tasks
    )
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


def get_sts_orgunit(uuid: str, settings):
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
        msg = f"Unit with {uuid=} is not a unit below {top_unit_uuid=}. Check the setting os2sync_top_unit_uuid."
        logger.error(msg)
        raise ValueError(msg)

    sts_org_unit = {"ItSystemUuids": [], "Name": base["name"], "Uuid": uuid}

    if base.get("parent") and "uuid" in base["parent"]:
        sts_org_unit["ParentOrgUnitUuid"] = base["parent"]["uuid"]

    itsystems_to_orgunit(
        sts_org_unit,
        os2mo_get("{BASE}/ou/" + uuid + "/details/it").json(),
        uuid_from_it_systems=settings.os2sync_uuid_from_it_systems,
    )
    addresses_to_orgunit(
        sts_org_unit,
        os2mo_get("{BASE}/ou/" + uuid + "/details/address").json(),
    )

    if settings.os2sync_sync_managers:
        manager_uuid = manager_to_orgunit(uuid)
        if manager_uuid:
            sts_org_unit["managerUuid"] = manager_uuid

    # this is set by __main__
    if has_kle():
        kle_to_orgunit(
            sts_org_unit,
            os2mo_get("{BASE}/ou/" + uuid + "/details/kle").json(),
            use_contact_for_tasks=settings.os2sync_use_contact_for_tasks,
        )

    # show_all_details(uuid,"ou")
    strip_truncate_and_warn(
        sts_org_unit, sts_org_unit, settings.os2sync_truncate_length
    )

    if settings.os2sync_uuid_from_it_systems:
        it = os2mo_get(f"{{BASE}}/ou/{uuid}/details/it").json()
        sts_org_unit["Uuid"] = get_fk_org_uuid(
            it, uuid, settings.os2sync_uuid_from_it_systems
        )
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
