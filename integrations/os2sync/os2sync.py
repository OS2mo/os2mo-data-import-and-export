# -- coding: utf-8 --
#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import requests
import logging
from integrations.os2sync import config
import hashlib
import json


settings = config.settings
logger = logging.getLogger(config.loggername)
hash_cache = {}
session = requests.Session()
session.verify = settings["OS2SYNC_CA_BUNDLE"]
session.headers = {
    "User-Agent": "os2mo-data-import-and-export",
    "CVR": settings["OS2SYNC_MUNICIPALITY"]
}


def already_xferred(url, params, method):
    params_hash = hashlib.sha224(
        (json.dumps(params, sort_keys=True) + method).encode("utf-8")
    ).hexdigest()
    if hash_cache.get(url) == params_hash:
        return True
    else:
        hash_cache[url] = params_hash
    return False


def os2sync_url(url):
    """format url like {BASE}/user
    """
    url = url.format(BASE=settings["OS2SYNC_API_URL"])
    return url


def os2sync_get(url, **params):
    url = os2sync_url(url)
    try:
        r = session.get(url, params=params)
        r.raise_for_status()
        return r
    except Exception:
        logger.error(url + " " + r.text)
        logger.exception("")
        raise


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
    except Exception:
        logger.error(url + " " + r.text)
        logger.exception("")
        raise


def os2sync_post(url, **params):
    url = os2sync_url(url)
    try:
        r = session.post(url, **params)
        r.raise_for_status()
        return r
    except Exception:
        logger.error(url + " " + r.text)
        logger.exception("")
        raise


def user_uuids():
    return os2sync_get("{BASE}/user").json()


def delete_user(uuid):
    if not already_xferred("/user/" + uuid, {}, "delete"):
        logger.info("delete user %s", uuid)
        os2sync_delete("{BASE}/user/" + uuid)
    else:
        logger.info("delete user %s - cached", uuid)


def upsert_user(user):
    if not already_xferred("/user/" + user["Uuid"], user, "upsert"):
        logger.info("upsert user %s", user["Uuid"])
        os2sync_post("{BASE}/user", json=user)
    else:
        logger.info("upsert user %s - cached", user["Uuid"])


def orgunit_uuids():
    return os2sync_get("{BASE}/orgunit").json()


def delete_orgunit(uuid):
    if not already_xferred("/orgunit/" + uuid, {}, "delete"):
        logger.info("delete orgunit %s", uuid)
        os2sync_delete("{BASE}/orgunit/" + uuid)
    else:
        logger.info("delete orgunit %s - cached", uuid)


def upsert_orgunit(org_unit):
    if not already_xferred("/orgunit/" + org_unit["Uuid"], org_unit, "upsert"):
        logger.info("upsert orgunit %s", org_unit["Uuid"])
        os2sync_post("{BASE}/orgunit/", json=org_unit)
    else:
        logger.info("upsert orgunit %s - cached", org_unit["Uuid"])
