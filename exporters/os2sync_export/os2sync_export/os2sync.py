# -- coding: utf-8 --
#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from uuid import UUID

import httpx
import requests
from os2sync_export import config
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_delay
from tenacity import wait_exponential
from tenacity import wait_fixed

retry_max_time = 10
settings = config.get_os2sync_settings()
logger = logging.getLogger(__name__)


def get_os2sync_session():

    session = requests.Session()

    if settings.os2sync_api_url == "stub":
        from os2ysnc_export import stub

        session = stub.Session()

    session.verify = settings.os2sync_ca_verify_os2sync
    session.headers["User-Agent"] = "os2mo-data-import-and-export"
    session.headers["CVR"] = settings.municipality
    return session


session = get_os2sync_session()


def changed(from_os2mo, from_os2sync):
    """Check if anything is changed in either of the keys that exists in OS2MO."""
    # Skip checking some keys:
    # "Uuid" as we know it is the same, and also it's called 'uuid' in the os2sync response for some reason
    # TODO: "ItSystemUuids" is not in the response. Might be fixed by upgrading os2sync #50261
    relevant_keys = set(from_os2mo.keys()) - set(["Uuid", "ItSystemUuids"])
    from_os2sync = {k[0].upper() + k[1:]: v for k, v in from_os2sync.items()}
    return any(from_os2mo[k] != from_os2sync[k] for k in relevant_keys)


def os2sync_url(url):
    """format url like {BASE}/user"""
    url = url.format(BASE=settings.os2sync_api_url)
    return url


def os2sync_delete(url, **params):
    url = os2sync_url(url)
    r = session.delete(url, **params)
    r.raise_for_status()
    return r


def os2sync_post(url, **params):
    url = os2sync_url(url)
    r = session.post(url, **params)
    r.raise_for_status()
    return r


def delete_user(uuid):
    logger.debug("delete user %s", uuid)
    os2sync_delete("{BASE}/user/" + uuid)


def upsert_user(user):
    logger.debug("upsert user %s", user["Uuid"])
    os2sync_post("{BASE}/user", json=user)


def delete_orgunit(uuid):
    logger.debug("delete orgunit %s", uuid)
    os2sync_delete("{BASE}/orgUnit/" + uuid)


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_delay(retry_max_time),
    retry=retry_if_exception_type(requests.HTTPError),
)
async def os2sync_get(client, url, **params):
    r = await client.get(url, **params)
    if r.status_code not in (200, 404):
        r.raise_for_status()
    return r.json()


async def upsert_orgunit(client: httpx.AsyncClient, org_unit):
    # Check data on unit before trying to sync
    from_os2sync = await os2sync_get(
        client, f"{settings.os2sync_api_url}/orgUnit/{org_unit['Uuid']}"
    )

    if changed(from_os2mo=org_unit, from_os2sync=from_os2sync):
        logger.debug(f"upsert orgunit {org_unit}")
        # We have no support for these fields in OS2MO yet so use whatever is in fk-org.
        org_unit["PayoutUnitUuid"] = from_os2sync["payoutUnitUuid"]
        org_unit["ContactForTasks"] = from_os2sync["contactForTasks"]
        return client.post(f"{settings.os2sync_api_url}/orgUnit/", json=org_unit)
    else:
        logger.debug("no changes to orgunit %s ", org_unit["Uuid"])


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
    stop=stop_after_delay(5 * 60),
    retry=retry_if_exception_type(requests.HTTPError),
)
def get_hierarchy(client: requests.Session, os2sync_api_url: str, request_uuid: UUID):
    """Fetches the hierarchy from os2sync. Retries for 5 minutes until it is ready"""
    r = client.get(f"{os2sync_api_url}/hierarchy/{str(request_uuid)}")
    r.raise_for_status()
    hierarchy = r.json()["result"]
    if hierarchy is None:
        raise ConnectionError("Check connection to FK-ORG")
    return hierarchy
