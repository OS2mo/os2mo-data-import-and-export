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
import requests
import uuid



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
        'street' : mh_address["Adresse"].replace(", " + pstnrby, "")
    }


def export_ou_emus(mh, nodes, emus_file):
    fieldnames = ['startDate', 'endDate', 'parentOrgUnit', 'manager',
        'longName', 'street', 'zipCode', 'city', 'phoneNumber']
    rows = []
    for node in cq.PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        manager = mh.read_organisation_managers(node.name)
        manager_uuid = manager["uuid"] if manager else ''
        import pdb; pdb.set_trace()
        address = get_emus_address(node.name)
        fra = ou['validity']['from'] if ou['validity']['from'] else ''
        til = ou['validity']['to'] if ou['validity']['to'] else ''
        over_uuid = ou['parent']['uuid'] if ou['parent'] else ''
        phone='',
        row = {
            'uuid': ou['uuid'],
            'startDate': fra,
            'endDate': til,
            'parentOrgUnit': over_uuid,
            'manager': manager_uuid,
            'longName': ou['name'],
            'street': address.get("street",''),
            'zipCode': address.get("zipCode",''),
            'city': address.get("city",''),
            'phoneNumber':phone,
        }
        rows.append(row)

    last_changed = datetime.datetime.strftime("%Y-%m-%d")
    for r in rows:
        emus_file.write("<orgUnit id=\"%s\" client=\"1\" lastChanged=\"%s\">\n" %(
            r["uuid"]
        ))
        for fn in fieldnames:
            emus_file.write("<%s>%s</%s>\n"%(fn, r.get(fn,''), fn))
        emus_file.write("</orgUnit>\n")

def main(
    root_org_unit_name="Læsø Kommune",
    threaded_speedup=False,
    mh=MoraHelper(),
    t=time.time(),
    emus_filename="emus_filename.xml",
):
    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    for root in roots:
        if root['name'] == root_org_unit_name:
            root_org_unit_uuid = root['uuid']

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    nodes = mh.read_ou_tree(root_org_unit_uuid)
    print('Read nodes: {}s'.format(time.time() - t))

    with open(emus_filename, "w", encoding="utf-8") as emus_xml:

        emus_xml.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
        emus_xml.write("<OS2MO>")

        export_ou_emus(mh, nodes, emus_xml)
        print('Writing Org Units: {}s'.format(time.time() - t))

        emus_xml.write("</OS2MO>")


if __name__ == '__main__':
    mh = MoraHelper("os2mo-28377:5000")
    #mh.read_organisationsenhed = mh.read_ou
    main(mh=mh)

