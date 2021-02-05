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

"""
SD Mox trigger.module
"""

import logging
import datetime
import json
import requests
import requests.exceptions
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


def mo_request(service, method="get", **params):
    method = getattr(requests, method)
    # :5000/service/ + "ou/1234"
    url = sdmox_config["OS2MO_SERVICE"] + service

    if sdmox_config["OS2MO_TOKEN"]:
        headers={"SESSION": sdmox_config["OS2MO_TOKEN"]}
    else:
        headers={}

    try:
        r = method(
            url,
            headers=headers,
            params=params,
            verify=sdmox_config["OS2MO_VERIFY"]
        )
        r.status_code == requests.codes.ok or r.raise_for_status()
        return r
    except Exception:
        logger.exception(url)
        raise


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
    mora_org = sdmox_config.get("ORG_UUID")
    if mora_org is None:
        mora_org = sdmox_config.setdefault("ORG_UUID", mo_request("o").json()[0]["uuid"])

    if (
        # we will never create at top level
        not data["request"]["parent"]
        or mora_org == data["request"]["parent"]["uuid"]
    ):
        return

    parent = mo_request("ou/" + data["request"]["parent"]["uuid"]).json()

    if not is_sd_triggered(parent):
        return

    # try to create a unit in sd
    from_date = datetime.datetime.strptime(
        data["request"]['validity']['from'], '%Y-%m-%d'
    )
    mox = sd_mox.sdMox.create(from_date=from_date)

    payload = mox.payload_create(data["uuid"], data["request"], parent)
    mox.create_unit(test_run=False, **payload)

    if not data["request"].get("details",[]):
        # check unit here
        mox.check_unit(operation="import", **payload)
    else:
        # check unit after editing addresses
        address_before_create(data, unit_given=True)


def ou_before_edit(data):
    """An ou is about to be renamed or moved."""
    from_date_str = data["request"]["data"]["validity"]["from"]
    unit = mo_request("ou/" + data["uuid"], at=from_date_str).json()
    if not is_sd_triggered(unit):
        return

    from_date = datetime.datetime.strptime(from_date_str, '%Y-%m-%d')
    mox = sd_mox.sdMox.create(from_date=from_date)
    if "name" in data["request"]["data"]:
        new_name = data["request"]["data"]["name"]
        mox.rename_unit(data["uuid"], new_Name, at=from_date, dry_run=True)
    elif "parent" in data["request"]["data"]:
        # doing a read department here will give the non-unique error
        # here - where we still have access to the mo-error reporting
        code_errors = mox._validate_unit_code(unit['user_key'], can_exist=True)
        if code_errors:
            raise sd_mox.SdMoxError(", ".join(code_errors))

        # we are moving a department
        parent = mo_request(
            "ou/" + data["request"]["data"]["parent"]["uuid"],
            at=from_date_str
        ).json()
        logger.debug("parent in ou_before_edit: {}".format(parent))
        payload = mox.payload_create(data["uuid"], unit, parent)
        operation = "flyt"
        mox.move_unit(test_run=False, **payload)
        # when moving, do not check against name
        payload["unit_name"] = None
        mox.check_unit(operation=operation, **payload)


def address_before_create(data, unit_given=False):
    """ Addresses are about to be created
        if unit is also new, it is given as a whole
    """

    # whole department changes validity - including addresses
    from_date = data["request"]["validity"]["from"]

    if unit_given:

        # a new unit has been created
        ou = data["uuid"]
        unit = data["request"]
        addresses = unit["details"]

    else:

        # a new address is being added to an existing unit
        ou = data.get("org_unit_uuid")
        if not ou:
            return
        try:
            unit = mo_request("ou/" + ou, at=from_date).json()
            if not is_sd_triggered(unit):
                return
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return  # new unit - checked elsewhere
            raise

        previous_addresses = mo_request(
            "ou/" + ou + "/details/address", at=from_date
        ).json()

        # the new address is prepended to addresses and
        # thereby given higher priority in sd_mox.py
        # see 'grouped_addresses'
        addresses = [data["request"]] + previous_addresses


    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = sd_mox.sdMox.create(from_date=from_date)

    payload = mox.payload_edit(ou, unit, addresses)
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
    mox = sd_mox.sdMox.create(from_date=from_date)

    # doing a read department here will give the non-unique error
    # here - where we still have access to the mo-error reporting
    code_errors = mox._validate_unit_code(unit['user_key'], can_exist=True)
    if code_errors:
        raise sd_mox.SdMoxError(", ".join(code_errors))

    payload = mox.payload_edit(ou, unit, addresses)
    mox.edit_unit(test_run=False, **payload)
    mox.check_unit(operation="ret", **payload)


def register(app):
    sdmox_config = sd_mox.read_sdmox_config()
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
