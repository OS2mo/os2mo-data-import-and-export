# 
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
SD Mox trigger.module
"""

import logging
import pathlib
import datetime
import sys
import json
import requests

# OS2MO imports
from mora.triggers import Trigger

# os2mo-data-import-and-export imports
import customer
custpath = pathlib.Path(customer.__file__).parent
sys.path.append(str(custpath))

from customer.integrations.SD_Lon import (
    sd_mox,
    sd_logging,
    sd_common
)

sdmox_config = {}

def read_config(app):
    cfg_file = custpath / "settings" / app.config["CUSTOMER_CONFIG_FILE"]
    cfg = json.loads(cfg_file.read_text())
    sdmox_config.update(
        sd_common.get_prefixed_configuration(
            cfg,
            sd_mox.CFG_PREFIX
        )
    )
    sdmox_config["sd_common"] = sd_common.get_prefixed_configuration(
        cfg,
        sd_common.CFG_PREFIX
    )


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
    # instantiate integration object
    from_date = datetime.datetime(2019, 7, 1, 0, 0)
    return sd_mox.sdMox(from_date, **sdmox_config)

#def dummy(*args, **kwargs):
#    return True
#sd_mox.sdMox._init_amqp_comm = dummy
#sd_mox.sdMox.call = dummy

def sd_mox_pretriggered(data):
    """ This is the function that is called with data from the handler module
    """

    # see if some parent uuid demands the trigger be run
    is_sd_triggered = False
    p = parent = mo_request("ou/" + data["request"]["parent"]["uuid"]).json()
    while p and p["uuid"]:
        if p["uuid"] in sdmox_config.get("TRIGGERED_UUIDS"):
            is_sd_triggered = True
            break
        p = p["parent"]

    if not is_sd_triggered:
        return


    # actual trigger code - create a unit in sd

    mox = get_sdMox()
    from_date = datetime.datetime.strptime(
        data["request"]['validity']['from'], '%Y-%m-%d'
    )
    mox._update_virkning(from_date)

    payload = mox.payload_create(data["uuid"], data["request"], parent)
    mox.create_unit(test_run=False, **payload)


def sd_mox_posttriggered(data):

    # see if some parent uuid demands the trigger be run
    is_sd_triggered = False
    p = unit = mo_request("ou/" + data["uuid"]).json()
    while p and p["uuid"]:
        if p["uuid"] in sdmox_config.get("TRIGGERED_UUIDS"):
            is_sd_triggered = True
            break
        p = p["parent"]

    if not is_sd_triggered:
        return

    # actual trigger code - edit a unit in sd

    mox = get_sdMox()
    from_date = datetime.datetime.strptime(
        unit['validity']['from'], '%Y-%m-%d'
    )
    mox._update_virkning(from_date)

    addresses = mo_request("ou/" + data["uuid"] + "/details/address").json()
    payload = mox.payload_edit(data["uuid"], unit, addresses)
    mox.edit_unit(**payload)


def register(app):
    read_config(app)

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.CREATE,
        Trigger.Event.ON_BEFORE
    )(sd_mox_pretriggered)

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.CREATE,
        Trigger.Event.ON_AFTER
    )(sd_mox_posttriggered)

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.EDIT,
        Trigger.Event.ON_AFTER
    )(sd_mox_posttriggered)


