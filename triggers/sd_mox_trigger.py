#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Triggerkoden i dette modul har to funktioner:
# 1) At oprette/rette/flytte en afdeling i SD, inden det sker i OS2mo
# 2) At forhinde oprettelse/flytning/rettelse i OS2mo, hvis det ikke
# lykkedes i SD
#
# Adressernes rækkefølge har betydning.
#     Der skal findes en postadresse inden man opretter et Pnummer,
#     ellers går tilbagemeldingen fra SD tilsyneladende i ged.
#     Der er indført et check for det i sd_mox.py
#
#

"""
SD Mox trigger.module
"""

import logging
import datetime
import json
import requests
import pprint
import pathlib

logger = logging.getLogger("sd_mox_trigger")

try:
    # if integrated we have a symbolic link in site-packages
    import customer
    import sys
    custpath = pathlib.Path(customer.__file__).parent
    sys.path.append(str(custpath))
except Exception:
    # else we must be testing
    pass

import integrations  # noqa
from integrations.SD_Lon import (
    sd_mox,
    sd
)  # noqa

sdmox_config = {}


def read_config(app):
    cfg_file = custpath / "settings" / "settings.json"
    cfg = json.loads(cfg_file.read_text())
    sdmox_config.update(
        sd.get_prefixed_configuration(
            cfg,
            sd_mox.CFG_PREFIX
        )
    )
    sdmox_config["sd_common"] = sd.get_prefixed_configuration(
        cfg,
        sd.CFG_PREFIX
    )
    # force no caching for sd
    sdmox_config["sd_common"]["USE_PICKLE_CACHE"] = False


def mo_request(service, method="get", **params):
    method = getattr(requests, method)
    # :5000/service/ + "ou/1234"
    url = sdmox_config["OS2MO_SERVICE"] + service
    try:
        r = method(
            url,
            headers={"SESSION": sdmox_config["OS2MO_TOKEN"]},
            params=params,
            verify=sdmox_config["OS2MO_VERIFY"]
        )
        r.status_code == requests.codes.ok or r.raise_for_status()
        return r
    except Exception:
        logger.exception(url)
        raise


def get_sdMox():
    """ instantiate integration object
    """
    from_date = datetime.datetime(2019, 7, 1, 0, 0)
    mox = sd_mox.sdMox(from_date, **sdmox_config)
    mox.amqp_connect()
    return mox


def is_sd_triggered(p):
    """ determine whether trigger code should run for unit p
    """
    while p and p["uuid"]:
        if p["uuid"] in sdmox_config.get("TRIGGERED_UUIDS"):
            return True
        p = p["parent"]
    return False


def ou_before_create(data):
    """ An ou is about to be created
    """
    parent = mo_request("ou/" + data["request"]["parent"]["uuid"]).json()
    if not is_sd_triggered(parent):
        return

    # try to create a unit in sd
    from_date = datetime.datetime.strptime(
        data["request"]['validity']['from'], '%Y-%m-%d'
    )
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_create(data["uuid"], data["request"], parent)
    mox.create_unit(test_run=False, **payload)
    mox.check_unit(operation="import", **payload)


def ou_before_edit(data):
    """ An ou is about to be renamed or moved
    """
    from_date_str = data["request"]["data"]["validity"]["from"]
    unit = mo_request("ou/" + data["uuid"], at=from_date_str).json()
    if not is_sd_triggered(unit):
        return

    from_date = datetime.datetime.strptime(from_date_str, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    if "name" in data["request"]["data"]:
        # we are renaming a department
        unit["name"] = data["request"]["data"]["name"]
        addresses = mo_request(
            "ou/" + data["uuid"] + "/details/address",
            at=from_date_str
        ).json()
        payload = mox.payload_edit(data["uuid"], unit, addresses)
        operation = "ret"
        mox.edit_unit(test_run=False, **payload)

    elif "parent" in data["request"]["data"]:
        # we are moving a department
        parent = mo_request(
            "ou/" + data["request"]["data"]["parent"]["uuid"],
            at=from_date_str
        ).json()
        payload = mox.payload_create(data["uuid"], unit, parent)
        operation = "flyt"
        mox.move_unit(test_run=False, **payload)
        # when moving, do not check against name
        payload["unit_name"] = None

    mox.check_unit(operation=operation, **payload)


def address_before_create(data):
    """ An address is about to be created
    """
    # addresses for persons are skipped
    ou = data.get("org_unit_uuid")
    if not ou:
        return

    from_date = data["request"]["validity"]["from"]
    unit = mo_request("ou/" + ou, at=from_date).json()
    if not is_sd_triggered(unit):
        return

    # the new address is prepended to addresses and
    # thereby given higher priority in sd_mox.py
    # see 'grouped_addresses'
    addresses = [data["request"]] + mo_request(
        "ou/" + ou + "/details/address", at=from_date
    ).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(ou, unit, addresses)
    # pprint.pprint(payload)
    mox.edit_unit(test_run=False, **payload)
    mox.check_unit(operation="ret", **payload)


def address_before_edit(data):
    """ An address is about to be changed
    """
    ou = data.get("org_unit_uuid")
    if not ou:
        return
    from_date = data["request"]["data"]["validity"]["from"]
    unit = mo_request("ou/" + ou, at=from_date).json()
    if not is_sd_triggered(unit):
        return

    # the changed address is prepended to addresses and
    # thereby given higher priority in sd_mox.py
    # see 'grouped_addresses'
    addresses = [data["request"]["data"]] + mo_request(
        "ou/" + ou + "/details/address", at=from_date
    ).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(ou, unit, addresses)
    mox.edit_unit(test_run=False, **payload)
    mox.check_unit(operation="ret", **payload)


def register(app):
    read_config(app)
    from mora.triggers import Trigger

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.CREATE,
        Trigger.Event.ON_BEFORE
    )(ou_before_create)

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.EDIT,
        Trigger.Event.ON_BEFORE
    )(ou_before_edit)

    Trigger.on(
        "address",
        Trigger.RequestType.CREATE,
        Trigger.Event.ON_BEFORE
    )(address_before_create)

    Trigger.on(
        "address",
        Trigger.RequestType.EDIT,
        Trigger.Event.ON_BEFORE
    )(address_before_edit)

    logger.warning("registered")
