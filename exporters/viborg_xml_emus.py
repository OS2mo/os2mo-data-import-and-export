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
import io
import logging
import collections
from xml.sax.saxutils import escape

MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:5000')
MORA_ROOT_ORG_UNIT_NAME = os.environ.get('MORA_ROOT_ORG_UNIT_NAME', 'Viborg Kommune')
USERID_ITSYSTEM = os.environ.get('USERID_ITSYSTEM', 'Active Directory')
EMUS_RESPONSIBILITY_CLASS = os.environ['EMUS_RESPONSIBILITY_CLASS']
EMUS_FILENAME = os.environ.get('EMUS_FILENAME', 'emus_filename.xml')
LOG_LEVEL = logging._nameToLevel.get(os.environ.get('LOG_LEVEL', 'WARNING'), 20)


logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
)

logger = logging.getLogger("xml-export-emus")
for i in logging.root.manager.loggerDict:
    if i in ["mora-helper", "xml-export-emus"]:
        logging.getLogger(i).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(i).setLevel(logging.WARNING)

engagement_counter = collections.Counter()


def get_emus_address(mh, ou_uuid):
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
        if not engagement_counter[ou["uuid"]]:
            logger.info("skipping dept %s with no non-hourly-paid employees",
                        ou["uuid"])
            continue

        manager = mh.read_ou_manager(node.name)
        manager_uuid = manager["uuid"] if manager else ''
        address = get_emus_address(mh, node.name)
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
        logger.info("adding ou %s", ou["uuid"])
        rows.append(row)

    last_changed = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info("writing %d ou rows to file", len(engagement_counter))
    for r in rows:
        empls = engagement_counter[r["uuid"]]
        if empls == 0:
            logger.debug("empty department skipped: %s (%s)",
                         r["longName"], r["uuid"])
        else:
            logger.debug("department %s (%s) has %s engagements incl. subdepts.",
                         r["longName"], r["uuid"], empls)
            emus_file.write(
                "<orgUnit id=\"%s\" client=\"1\" lastChanged=\"%s\">\n" % (
                    r["uuid"],
                    last_changed,
                )
            )

            for fn in fieldnames:
                emus_file.write("<%s>%s</%s>\n" % (fn, escape(r.get(fn, '')), fn))

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


"""
musskema adaptation
musskema, the application receiving this export, did not have a manager
object for import therefore the employee has been used for that too -
and a few fields in the exported employee object are changed accordingly.
Render_engagement will be used to render both types as an engagement,
and then, for managers, the resulting row wil be adapted
"""


def build_engagement_row(mh, ou, engagement):
    entrydate = engagement.get("validity", {}).get("from")
    leavedate = engagement.get("validity", {}).get("to")
    employee = mh._mo_lookup(
        engagement["person"]["uuid"],
        'e/{}'
    )

    if "surname" in engagement["person"]:
        firstname = engagement["person"]["givenname"]
        lastname = engagement["person"]["surname"]
    else:
        firstname, lastname = engagement["person"]["name"].rsplit(
            " ", maxsplit=1
        )

    username = mh.get_e_username(
        engagement["person"]["uuid"],
        'Active Directory'
    )
    _phone = mh.get_e_address(engagement["person"]["uuid"], "PHONE")
    _email = mh.get_e_address(engagement["person"]["uuid"], "EMAIL")

    row = {
        # employee_id is tjenestenr by default
        'employee_id': engagement.get("user_key", ''),
        # client is 1 by default
        'client': "1",
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
    return row


def get_manager_dates(mh, person):
    startdate = '9999-12-31'
    enddate = '0000-00-00'
    for engagement in mh.read_user_engagement(person["uuid"], read_all=True):
        if engagement["validity"].get("to") and enddate is not '':
            # Enddate is finite, check if it is later than current
            if engagement["validity"]["to"] > enddate:
                enddate = engagement["validity"]["to"]
        else:
            enddate = ''

        if engagement["validity"]["from"] < startdate:
            startdate = engagement["validity"]["from"]

    assert startdate < '9999-12-31'
    assert enddate is '' or enddate > '0000-00-00'
    return startdate, enddate


def build_manager_rows(mh, ou, manager):
    # render manager returns a list as a manager will typically have more
    # responsibility areas and musskema requires one for each

    rows = []
    person = manager["person"]

    employee = mh._mo_lookup(
        person["uuid"],
        'e/{}'
    )
    entrydate, leavedate = get_manager_dates(mh, person)

    if "surname" in person:
        firstname = person["givenname"]
        lastname = person["surname"]
    else:
        firstname, lastname = person["name"].rsplit(
            " ", maxsplit=1
        )

    username = mh.get_e_username(
        person["uuid"],
        'Active Directory'
    )

    _phone = mh.get_e_address(person["uuid"], "PHONE")
    _email = mh.get_e_address(person["uuid"], "EMAIL")

    # manipulate row into a manager row
    # empty a couple of fields, change client and employee_id
    # and manipulate from and to

    for responsibility in manager["responsibility"]:
        if not responsibility["uuid"] == EMUS_RESPONSIBILITY_CLASS:
            logger.debug("skipping man. resp. %s", responsibility["name"])
            continue
        logger.info(
            "adding manager %s with man. resp. %s",
            manager["uuid"],
            responsibility["name"]
        )
        row = {
            'employee_id': person["uuid"],
            'client': "540",
            'entryDate': entrydate,
            'leaveDate': leavedate,
            'cpr': employee['cpr_no'],
            'firstName': firstname,
            'lastName': lastname,
            'workPhone': _phone.get("name", '') if _phone.get(
                "visibility", {}
            ).get("scope", "") != "SECRET" else 'hemmelig',
            'workContract': '',
            'workContractText': '',
            'positionId': '',
            'position': responsibility["name"],
            'orgUnit': '',
            'email': _email.get("name", "") if _email.get(
                "visibility", {}
            ).get("scope", "") != "SECRET" else 'hemmelig',
            'username': username,
        }
        rows.append(row)
    return rows


def hourly_paid(engagement):
    "workers hourly paid are determined by user_key prefix"
    hp = engagement["user_key"][0] in ["8", "9"]
    if hp:
        logger.info("engagement %s with user_key %s considered hourly paid",
                    engagement["uuid"], engagement["user_key"])
    return hp


def engagement_count(mh, ou):
    engagement_counter[ou["uuid"]] += 1
    parent = ou
    while parent and parent.get("uuid") and parent.get("parent"):
        parent = mh.read_ou(parent["parent"]["uuid"])
        engagement_counter[parent["uuid"]] += 1


def export_e_emus(mh, nodes, emus_file):
    fieldnames = ['entryDate', 'leaveDate', 'cpr', 'firstName',
                  'lastName', 'workPhone', 'workContract', 'workContractText',
                  'positionId', 'position', "orgUnit", 'email', "username"]
    manager_rows = []
    engagement_rows = []

    for node in cq.PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)

        # normal engagements - original export
        for engagement in mh._mo_lookup(
                ou["uuid"],
                'ou/{}/details/engagement'
        ):
            if hourly_paid(engagement):
                continue
            logger.info("adding engagement %s", engagement["uuid"])
            engagement_rows.append(build_engagement_row(mh, ou, engagement))
            engagement_count(mh, ou)

        # manager engagements - above mentioned musskema adaptation
        for manager in mh._mo_lookup(
                ou["uuid"],
                'ou/{}/details/manager'
        ):
            if not manager.get("person"):
                logger.info("skipping vacant manager %s", manager["uuid"])
                continue  # vacant manager
            else:
                # extend, as there can be zero or one
                manager_rows.extend(build_manager_rows(mh, ou, manager))

    if not len(manager_rows):
        logger.error("no managers found - did You forget to"
                     " specify correct EMUS_RESPONSIBILITY_CLASS")
    rows = engagement_rows + manager_rows
    logger.info("writing %d engagement rows and %d manager rows to file",
                len(engagement_rows), len(manager_rows))
    last_changed = datetime.datetime.now().strftime("%Y-%m-%d")
    for r in rows:
        emus_file.write("<employee id=\"%s\" client=\"%s\" lastChanged=\"%s\">\n" % (
            r["employee_id"],
            r["client"],
            last_changed,
        ))
        for fn in fieldnames:
            emus_file.write("<%s>%s</%s>\n" % (fn, escape(r.get(fn, '')), fn))
        emus_file.write("</employee>\n")


def main(
    emus_xml_file,
    root_org_unit_name=MORA_ROOT_ORG_UNIT_NAME,
    mh=MoraHelper(),
    t=time.time(),
):
    root_org_unit_uuid = None
    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    for root in roots:
        logger.debug("checking if %s is %s", root["name"], root_org_unit_name)
        if root['name'] == root_org_unit_name:
            root_org_unit_uuid = root['uuid']
            break

    if not root_org_unit_uuid:
        logger.error("%s not found in root-ous", root_org_unit_name)
        exit(1)

    logger.warning("caching all ou's,"
                   " so program may seem unresponsive temporarily")

    nodes = mh.read_ou_tree(root_org_unit_uuid)

    # Write the xml file
    emus_xml_file.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
    emus_xml_file.write("<OS2MO>\n")

    # had to switch the sequence - write e to tmp before append after ou
    temp_file = io.StringIO()
    export_e_emus(mh, nodes, temp_file)
    export_ou_emus(mh, nodes, emus_xml_file)

    emus_xml_file.write(temp_file.getvalue())

    emus_xml_file.write("</OS2MO>")


if __name__ == '__main__':
    morah = MoraHelper(MORA_BASE)
    with open(EMUS_FILENAME, "w", encoding="utf-8") as emus_f:
        main(emus_xml_file=emus_f, mh=morah)
