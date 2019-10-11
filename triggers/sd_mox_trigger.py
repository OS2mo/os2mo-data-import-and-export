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
import datetime
import json
import requests
import pprint
import pathlib

try:
    import customer
    import sys
    custpath = pathlib.Path(customer.__file__).parent
    sys.path.append(str(custpath))
except:
    # we must be testing
    pass

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
    mox.create_unit(**payload, test_run=False)


def ou_after_rename(data):
    """ an ou has been renamed, transfer name and addresses
    """
    from_date = data["request"]["data"]["validity"]["from"]
    p = unit = mo_request("ou/" + data["uuid"], at=from_date).json()
    if not is_sd_triggered(p):
        return

    addresses = mo_request("ou/" + data["uuid"] + "/details/address", at=from_date).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(data["uuid"], unit, addresses)
    pprint.pprint(payload)
    mox.edit_unit(**payload, test_run=False)


def address_create_after(data):
    """ an address has been created/changed - transfer it to sd if:
        * the address is for an ou
        * the ou is triggered/has a triggered parent
    """
    ou = data.get("org_unit_uuid")
    if not ou:
        return
    from_date = data["request"]["validity"]["from"]
    p = unit = mo_request("ou/" + data["org_unit_uuid"], at=from_date).json()
    if not is_sd_triggered(p):
        return

    addresses = mo_request("ou/" + ou + "/details/address", at=from_date).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(data["uuid"], unit, addresses)
    pprint.pprint(payload)
    mox.edit_unit(**payload, test_run=False)


def address_edit_after(data):
    """ an address has been created/changed - transfer it to sd if:
        * the address is for an ou
        * the ou is triggered/has a triggered parent
    """
    ou = data.get("org_unit_uuid")
    if not ou:
        return
    from_date = data["request"]["data"]["validity"]["from"]
    p = unit = mo_request("ou/" + data["org_unit_uuid"], at=from_date).json()
    if not is_sd_triggered(p):
        return

    addresses = mo_request("ou/" + ou + "/details/address", at=from_date).json()
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)

    payload = mox.payload_edit(data["uuid"], unit, addresses)
    mox.edit_unit(**payload, test_run=False)

def ret_med_ivan(from_date, payload):
    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    mox = get_sdMox()
    mox._update_virkning(from_date)
    pprint.pprint(payload)
    mox.edit_unit(**payload, test_run=False)

def register(app):
    from mora.triggers import Trigger

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.CREATE,
        Trigger.Event.ON_BEFORE
    )(ou_before_create)

    Trigger.on(
        Trigger.ORG_UNIT,
        Trigger.RequestType.EDIT,
        Trigger.Event.ON_AFTER
    )(ou_after_rename)

    Trigger.on(
        "address",
        Trigger.RequestType.CREATE,
        Trigger.Event.ON_AFTER
    )(address_create_after)

    Trigger.on(
        "address",
        Trigger.RequestType.EDIT,
        Trigger.Event.ON_AFTER
    )(address_edit_after)


if __name__ == "__main__":
    custpath = pathlib.Path(".")
    class App:
        config = {"CUSTOMER_CONFIG_FILE": "kommune-andeby.json"}

    read_config(App)
    ret_med_ivan("2019-11-01", {
        'adresse': {'silkdata:AdresseNavn': 'Toftebjerghaven 6',
             'silkdata:ByNavn': 'Ballerup',
             'silkdata:PostKodeIdentifikator': '2750'},
        'integration_values': {'formaalskode': '12345',
                        'skolekode': '12347',
                        'time_planning': 'Arbejdstidsplaner'},
        'name': 'LU - OS2MO Hejsa',
        'phone': '12222223',
        'pnummer': '1011600936',
        'unit_code': 'LU75',
        'unit_uuid': '08c6254f-6b64-4500-8a00-00000671LU75'})
