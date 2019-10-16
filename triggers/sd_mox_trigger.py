# 
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Adressernes rækkefølge har betydning.
# Der skal findes en postadresse inden man opretter et Pnummer, ellers går dert i ged
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
import sys

try:
    import customer
    import sys
    custpath = pathlib.Path(customer.__file__).parent
    sys.path.append(str(custpath))
except:
    # we must be testing
    pass

import integrations
from integrations.SD_Lon import (
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
    mox = sd_mox.sdMox(from_date, **sdmox_config)
    mox.amqp_connect()
    return mox

def dummy_init(self, *args, **kwargs):
    pprint.pprint((args, kwargs))

def dummy_call(self, xmlstr, *args):
    import xml.dom.minidom
    dom = xml.dom.minidom.parseString(xmlstr.encode("utf-8"))
    pretty_xml_as_string = dom.toprettyxml()
    print(pretty_xml_as_string, args)
#sd_mox.sdMox._init_amqp_comm = dummy_init
#sd_mox.sdMox.call = dummy_call


def is_sd_triggered(p):
    while p and p["uuid"]:
        if p["uuid"] in sdmox_config.get("TRIGGERED_UUIDS"):
            return True
        p = p["parent"]
    return False


def ou_before_create(data):
    """ An ou is about to be created
    """
    p = parent = mo_request("ou/" + data["request"]["parent"]["uuid"]).json()
    if not is_sd_triggered(p):
        return

    # try to create a unit in sd
    from_date = datetime.datetime.strptime(
        data["request"]['validity']['from'], '%Y-%m-%d'
    )
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_create(data["uuid"], data["request"], parent)
    pprint.pprint(payload)
    mox.create_unit(test_run=False, **payload)
    mox.check_unit(**payload)


def ou_before_edit(data):
    """ an ou has been renamed or moved
    """
    from_date_str = data["request"]["data"]["validity"]["from"]
    p = unit = mo_request("ou/" + data["uuid"], at=from_date_str).json()
    if not is_sd_triggered(p):
        return

    from_date = datetime.datetime.strptime(from_date_str, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    # rename seems to work, move not so much
    if "name" in data["request"]["data"]:
        unit["name"]  = data["request"]["data"]["name"]
        addresses = mo_request(
            "ou/" + data["uuid"] + "/details/address",
            at=from_date_str
        ).json()
        payload = mox.payload_edit(data["uuid"], unit, addresses)
        mox.edit_unit(test_run=False, **payload)

    elif "parent" in data["request"]["data"]:
        mox._update_virkning(from_date, datetime.datetime(2099,12,31))
        parent = mo_request(
            "ou/" + data["request"]["data"]["parent"]["uuid"],
            at=from_date_str
        ).json()
        payload = mox.payload_create(data["uuid"], unit, parent)
        mox.move_unit(test_run=False, **payload)

    pprint.pprint(payload)
    mox.check_unit(**payload)


def address_before_create(data):
    """ an address has been created/changed - transfer it to sd if:
        * the address is for an ou
        * the ou is triggered/has a triggered parent
    """
    ou = data.get("org_unit_uuid")
    if not ou:
        return
    from_date = data["request"]["validity"]["from"]
    p = unit = mo_request("ou/" + ou, at=from_date).json()
    if not is_sd_triggered(p):
        return

    # the new address is prepended to addresses and thereby given higher priority
    # see scoped and keyed addresses in sd_mox.py
    addresses = [data["request"]] + mo_request(
        "ou/" + ou + "/details/address", at=from_date
    ).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(ou, unit, addresses)
    pprint.pprint(payload)
    mox.edit_unit(test_run=False, **payload)
    mox.check_unit(**payload)


def address_before_edit(data):
    """ an address has been created/changed - transfer it to sd if:
        * the address is for an ou
        * the ou is triggered/has a triggered parent
    """
    ou = data.get("org_unit_uuid")
    if not ou:
        return
    from_date = data["request"]["data"]["validity"]["from"]
    p = unit = mo_request("ou/" + ou, at=from_date).json()
    if not is_sd_triggered(p):
        return

    # the edited address is prepended to addresses and thereby given higher priority
    # see scoped and keyed addresses in sd_mox.py
    addresses = [data["request"]["data"]] + mo_request(
        "ou/" + ou + "/details/address", at=from_date
    ).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(ou, unit, addresses)
    mox.edit_unit(test_run=False, **payload)
    mox.check_unit(**payload)

def ret_med_ivan(from_date, payload):
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)
    pprint.pprint(payload)
    mox.edit_unit(**payload, test_run=False)
    pprint.pprint(mox.check_unit(**payload))

def call_robert(from_date, uuid):
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)
    pprint.pprint(mox.read_parent(uuid))


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


if __name__ == "__main__":
    custpath = pathlib.Path(".")
    class App:
        config = {"CUSTOMER_CONFIG_FILE": sys.argv[1]}
    read_config(App) #3.22.01
    call_robert("2019-11-01","ad3b28aa-2998-43d9-8840-264f35a0fd82")

    ret_med_ivan("2019-11-01", {
        'adresse': {'silkdata:AdresseNavn': 'Toftebjerghaven 4',
             'silkdata:ByNavn': 'Ballerup',
             'silkdata:PostKodeIdentifikator': '2750'},
        'integration_values': {'formaalskode': '',
                        'skolekode': '',
                        'time_planning': 'Arbejdstidsplaner'},
        'name': 'LU - OS2MO Hejsa',
        'phone': '12341234',
        'pnummer': '1011600936',
        'unit_code': 'LU75',
        'unit_uuid': '08c6254f-6b64-4500-8a00-00000671LU75'})
