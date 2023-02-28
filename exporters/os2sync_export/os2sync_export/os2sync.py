# -- coding: utf-8 --
#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
import json
import logging
from typing import Dict
from typing import Optional
from typing import Tuple
from uuid import UUID

import requests
from os2sync_export import config
from os2sync_export.os2sync_models import orgUnit
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_delay
from tenacity import wait_fixed

retry_max_time = 60
settings = config.get_os2sync_settings()
logger = logging.getLogger(__name__)
hash_cache: Dict = {}


def get_os2sync_session():

    session = requests.Session()

    if settings.os2sync_api_url == "stub":
        from os2sync_export import stub

        session = stub.Session()

    session.verify = settings.os2sync_ca_verify_os2sync
    session.headers["User-Agent"] = "os2mo-data-import-and-export"
    session.headers["CVR"] = settings.municipality
    return session


session = get_os2sync_session()


def already_xferred(url, params, method):
    if settings.os2sync_api_url == "stub":
        params_hash = params
    else:
        params_hash = hashlib.sha224(
            (json.dumps(params, sort_keys=True) + method).encode("utf-8")
        ).hexdigest()
    if hash_cache.get(url) == params_hash:
        return True
    else:
        hash_cache[url] = params_hash
    return False


def os2sync_url(url):
    """format url like {BASE}/user"""
    url = url.format(BASE=settings.os2sync_api_url)
    return url


def os2sync_get(url, **params):
    url = os2sync_url(url)
    r = session.get(url, params=params)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def os2sync_get_org_unit(api_url: str, uuid: UUID) -> Optional[orgUnit]:
    current = os2sync_get(f"{api_url}/orgUnit/{str(uuid)}")
    if current is None:
        return None
    return orgUnit(**current)


def os2sync_delete(url, **params):
    url = os2sync_url(url)
    try:
        r = session.delete(url, **params)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning("delete %r %r :404", url, params)
            return r


def os2sync_post(url, **params):
    url = os2sync_url(url)
    r = session.post(url, **params)
    r.raise_for_status()
    return r


def delete_user(uuid):
    if not already_xferred("/user/" + uuid, {}, "delete"):
        logger.debug("delete user %s", uuid)
        os2sync_delete("{BASE}/user/" + uuid)
    else:
        logger.debug("delete user %s - cached", uuid)


def upsert_user(user):
    if not already_xferred("/user/" + user["Uuid"], user, "upsert"):
        logger.debug("upsert user %s", user["Uuid"])
        os2sync_post("{BASE}/user", json=user)
    else:
        logger.debug("upsert user %s - cached", user["Uuid"])


def delete_orgunit(uuid):
    if not already_xferred("/orgUnit/" + uuid, {}, "delete"):
        logger.debug("delete orgunit %s", uuid)
        os2sync_delete("{BASE}/orgUnit/" + uuid)
    else:
        logger.debug("delete orgunit %s - cached", uuid)


def upsert_orgunit(org_unit: orgUnit, os2sync_api_url, dry_run: bool = False) -> bool:
    current = os2sync_get_org_unit(api_url=os2sync_api_url, uuid=org_unit.Uuid)

    if not current:
        logger.info(f"OrgUnit not found in os2sync - creating {org_unit.Uuid=}")
        os2sync_post("{BASE}/orgUnit/", json=org_unit.json())
        return True

    # Avoid overwriting information that we cannot provide from os2mo.
    org_unit.LOSShortName = current.LOSShortName
    org_unit.Tasks = org_unit.Tasks or current.Tasks
    org_unit.ShortKey = org_unit.ShortKey or current.ShortKey
    org_unit.PayoutUnitUuid = org_unit.PayoutUnitUuid or current.PayoutUnitUuid
    org_unit.ContactPlaces = org_unit.ContactPlaces or current.ContactPlaces

    if dry_run:
        logger.info(f"Found changes to {org_unit.Uuid=}? {current != org_unit}")
        return current != org_unit
    if current == org_unit:
        logger.debug(f"No changes to {org_unit.Uuid=}")
        return False

    logger.info(f"Syncing org_unit {org_unit}")
    os2sync_post("{BASE}/orgUnit/", json=org_unit.json())
    return True


def trigger_hierarchy(client: requests.Session, os2sync_api_url: str) -> UUID:
    """ "Triggers a job in the os2sync container that gathers the entire hierarchy from FK-ORG

    Returns: UUID

    """
    r = client.get(f"{os2sync_api_url}/hierarchy")
    r.raise_for_status()
    return UUID(r.text)


@retry(
    wait=wait_fixed(5),
    reraise=True,
    stop=stop_after_delay(10 * 60),
    retry=retry_if_exception_type(requests.HTTPError),
)
def get_hierarchy(
    client: requests.Session, os2sync_api_url: str, request_uuid: UUID
) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """Fetches the hierarchy from os2sync. Retries for 10 minutes until it is ready"""
    r = client.get(f"{os2sync_api_url}/hierarchy/{str(request_uuid)}")
    r.raise_for_status()
    hierarchy = r.json()["result"]
    if hierarchy is None:
        raise ConnectionError("Check connection to FK-ORG")
    existing_os2sync_org_units = {o["uuid"]: o for o in hierarchy["oUs"]}
    existing_os2sync_users = {u["uuid"]: u for u in hierarchy["users"]}
    return existing_os2sync_org_units, existing_os2sync_users
