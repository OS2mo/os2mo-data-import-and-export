#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO.
These are specfic for Viborg
"""

import time
from os2mo_helpers.mora_helpers import MoraHelper
import common_queries as cq
import datetime
import requests
import uuid
import os

MORA_BASE = os.environ.get('MORA_BASE', 'localhost:80')
MORA_ROOT_ORG_UNIT_NAME = os.environ.get('MORA_ROOT_ORG_UNIT_NAME', 'Viborg Kommune')
USERID_ITSYSTEM = os.environ.get('USERID_ITSYSTEM', 'Active Directory')
EMUS_FILENAME = os.environ.get('EMUS_FILENAME', 'emus_filename.xml')


def get_emus_address(ou_uuid):
    """ try both adresse and adgangsadresse
    return {} if not found or uuid is falsy
    """
    mh_address = mh.read_ou_address(ou_uuid)
    adr_uuid = mh_address.get("value")
    if not adr_uuid or adr_uuid != str(uuid.UUID(adr_uuid)):
        return {}
    response = requests.get(
        "http://dawa.aws.dk/adresser?struktur=mini&id=%s" % adr_uuid)
    if not len(response.json()):
        response = requests.get(
            "http://dawa.aws.dk/adgangsadresser?struktur=mini&id=%s" % adr_uuid)
    if not len(response.json()) == 1:
        return {}
    address = response.json()[0]

    pstnrby = " ".join([
        address["postnr"],
        address["postnrnavn"],
     ])

    return {
        'zipCode': address["postnr"],
        'city': address["postnrnavn"],
        'street': mh_address["Adresse"].replace(", " + pstnrby, "")
    }


def export_ou_emus(mh, nodes, emus_file):
    fieldnames = ['startDate', 'endDate', 'parentOrgUnit', 'manager',
                  'longName', 'street', 'zipCode', 'city', 'phoneNumber']

    rows = []
    for node in cq.PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        manager = mh.read_organisation_managers(node.name)
        manager_uuid = manager["uuid"] if manager else ''
        address = get_emus_address(node.name)
        fra = ou['validity']['from'] if ou['validity']['from'] else ''
        til = ou['validity']['to'] if ou['validity']['to'] else ''
        over_uuid = ou['parent']['uuid'] if ou['parent'] else ''
        phone = mh.read_ou_address(
            ou["uuid"], scope="PHONE"
        ).get("Adresse", "")

        row = {
            'uuid': ou['uuid'],
            'startDate': fra,
            'endDate': til,
            'parentOrgUnit': over_uuid,
            'manager': manager_uuid,
            'longName': ou['name'],
            'street': address.get("street", ''),
            'zipCode': address.get("zipCode", ''),
            'city': address.get("city", ''),
            'phoneNumber': phone,
        }
        rows.append(row)

    last_changed = datetime.datetime.now().strftime("%Y-%m-%d")
    for r in rows:
        emus_file.write(
            "<orgUnit id=\"%s\" client=\"1\" lastChanged=\"%s\">\n" % (
                r["uuid"],
                last_changed,
            )
        )

        for fn in fieldnames:
            emus_file.write("<%s>%s</%s>\n" % (fn, r.get(fn, ''), fn))

        emus_file.write("</orgUnit>\n")


def get_e_username(e_uuid, id_it_system, mh):
    for its in mh._mo_lookup(e_uuid, 'e/{}/details/it'):
        if its['itsystem']["user_key"] == id_it_system:
            return its['user_key']
    return ''


def get_e_address(e_uuid, scope, mh):
    for address in mh._mo_lookup(e_uuid, 'e/{}/details/address'):
        if address['address_type']['scope'] == scope:
            return address
    return {}


def export_e_emus(mh, nodes, emus_file):
    fieldnames = ['entryDate', 'leaveDate', 'cpr', 'firstName',
                  'lastName', 'workPhone', 'workContract', 'workContractText',
                  'positionId', 'position', "orgUnit", 'email', "username"]
    rows = []

    for node in cq.PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        for engagement in mh._mo_lookup(
                ou["uuid"],
                'ou/{}/details/engagement'
        ):
            entrydate = engagement.get("validity", {}).get("from")
            leavedate = engagement.get("validity", {}).get("to")
            employee = mh._mo_lookup(
                engagement["person"]["uuid"],
                'e/{}'
            )
            firstname, lastname = engagement["person"]["name"].rsplit(
                " ", maxsplit=1
            )
            username = get_e_username(
                engagement["person"]["uuid"],
                'Active Directory',
                mh
            )
            _phone = get_e_address(engagement["person"]["uuid"], "PHONE", mh)
            _email = get_e_address(engagement["person"]["uuid"], "EMAIL", mh)

            row = {
                'tjenestenr': engagement.get("user_key", ''),
                'entryDate': entrydate if entrydate else '',
                'leaveDate': leavedate if leavedate else '',
                'cpr': employee['cpr_no'],
                'firstName': firstname,
                'lastName': lastname,
                'workPhone': _phone.get("name", '') if _phone.get(
                    "visibility", {}
                ).get("scope", "") != "SECRET" else 'hemmelig',
                'workContract': engagement.get(
                    "engagement_type", {}
                ).get("uuid", ""),
                'workContractText': engagement.get(
                    "engagement_type", {}
                ).get("name", ""),
                'positionId': engagement.get("job_function", {}).get("uuid", ""),
                'position': engagement.get("job_function", {}).get("name", ""),
                'orgUnit': ou["uuid"],
                'email': _email.get("name", "") if _email.get(
                    "visibility", {}
                ).get("scope", "") != "SECRET" else 'hemmelig',
                'username': username,
            }

            rows.append(row)

    last_changed = datetime.datetime.now().strftime("%Y-%m-%d")
    for r in rows:
        emus_file.write("<employee id=\"%s\" client=\"1\" lastChanged=\"%s\">\n" % (
            r["tjenestenr"],
            last_changed,
        ))
        for fn in fieldnames:
            emus_file.write("<%s>%s</%s>\n" % (fn, r.get(fn, ''), fn))
        emus_file.write("</employee>\n")


def main(
    root_org_unit_name=MORA_ROOT_ORG_UNIT_NAME,
    mh=MoraHelper(),
    t=time.time(),
    emus_filename=EMUS_FILENAME
):
    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    for root in roots:
        if root['name'] == root_org_unit_name:
            root_org_unit_uuid = root['uuid']

    nodes = mh.read_ou_tree(root_org_unit_uuid)

    with open(emus_filename, "w", encoding="utf-8") as emus_xml:

        emus_xml.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
        emus_xml.write("<OS2MO>\n")

        export_ou_emus(mh, nodes, emus_xml)
        export_e_emus(mh, nodes, emus_xml)

        emus_xml.write("</OS2MO>")


if __name__ == '__main__':
    mh = MoraHelper(MORA_BASE)
    main(mh=mh)
