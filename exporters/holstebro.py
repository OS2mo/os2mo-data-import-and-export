#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Make a number of pre-defined queries into MO.
In the abcense of known information about the dataset, the deepest root
tree is assumed to be the one to be exported.
These are for the general case, specific version for each municipality
is typically a better solution.

Script requires environment variables to be set. 
To get SAML_TOKEN you have to login to the OS2MO server (MORA_BASE) that is being used 
and subsequently access <server>/saml/api-token 
"""
import os
import queue
import threading
import time

import click
from anytree import PreOrderIter

import common_queries as cq
from os2mo_helpers.mora_helpers import MoraHelper

MORA_BASE = os.environ.get('MORA_BASE')
MO_ROOT_UUID = os.environ.get('MO_ROOT_UUID')
SAML_TOKEN = os.environ.get('SAML_TOKEN')


def export_from_mo(root, threaded_speedup, hostname):
    threaded_speedup = threaded_speedup
    t = time.time()

    mh = MoraHelper(hostname=hostname, export_ansi=False)

    org = mh.read_organisation()
    roots = mh.read_top_units(org)

    itdig_uuid = '9f981b4d-66c3-4100-b800-000001480001'

    # print(roots)
    for root in roots:
        if root['name'] == 'Holstebro Kommune':
            holstebro_uuid = root['uuid']

    #nodes = mh.read_ou_tree(holstebro_uuid)
    nodes = mh.read_ou_tree(itdig_uuid)
    print('Read nodes: {}s'.format(time.time() - t))

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    filename_org = 'planorama_org.csv'
    filename_persons = 'planorama_persons.csv'
    export_to_planorama(mh, nodes, filename_org, filename_persons)
    print('planorama_org.csv: {}s'.format(time.time() - t))

    """
    filename = 'org_incl-medarbejdere.csv'
    cq.export_orgs(mh, nodes, filename)
    print('org_incl-medarbejdere.csv: {}s'.format(time.time() - t))
    
    filename = 'alle_lederfunktioner_os2mo.csv'
    cq.export_managers(mh, nodes, filename)
    print('Alle ledere: {}s'.format(time.time() - t))
    

    filename = 'alle-medarbejdere-stilling-email_os2mo.csv'
    cq.export_all_employees(mh, nodes, filename)
    print('alle-medarbejdere-stilling-email_os2mo.csv: {}s'.format(time.time() - t))

    
    filename = 'adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv'
    cq.export_adm_org(mh, nodes, filename)
    print('adm-org-incl-start-stop: {}s'.format(time.time() - t))

    filename = 'tilknytninger.csv'
    cq.export_all_teams(mh, nodes, filename)
    print('tilknytninger: {}s'.format(time.time() - t))
    """


def export_to_planorama(mh, nodes, filename_org, filename_persons):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    :param nodes: The nodes of the OU tree
    """
    fieldnames_persons = ['UUID', 'Username', 'Password', 'Name', 'Title',
                          'Address', 'Zip', 'Country', 'CPR', 'Email',
                          'Number', 'Mobile', 'Telephone', 'Responsible', 'Company']
    fieldnames_org = ['Root', 'Number', 'Name']

    rows_org = []
    rows_persons = []

    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        # Do not add "Afdelings-niveau"
        if ou['org_unit_type']['name'] != 'Afdelings-niveau':
            fra = ou['validity']['from'] if ou['validity']['from'] else ''
            til = ou['validity']['to'] if ou['validity']['to'] else ''
            over_uuid = ou['parent']['uuid'] if ou['parent'] else ''
            row_org = {'Root': over_uuid,
                       'Number': ou['uuid'],
                       'Name': ou['name']}
            rows_org.append(row_org)

            employees = mh.read_organisation_people(node.name, 'engagement', False)
            # Does this node have a new name?
            manager = find_org_manager(mh, node)

            for uuid, employee in employees.items():
                row = {}
                address = mh.read_user_address(uuid, username=True, cpr=True)
                # row.update(address)  # E-mail, Telefon, Brugernavn, CPR NR
                # row.update(employee)  # Everything else
                row = {'UUID': uuid,
                       'Username': employee['User Key'],
                       'Password': '',
                       'Name': employee['Navn'],
                       'Title': employee['Stillingsbetegnelse'],
                       'Address': address['Lokation'] if 'Lokation' in address else '',
                       'Zip': '',
                       'Country': '',
                       'CPR': '',  # we do not send CPR to planorama
                       # 'CPR': address['CPR-Nummer'],
                       'Email': address['E-mail'] if 'E-mail' in address else '',
                       'Number': '',
                       'Mobile': address['Mobiltelefon'] if 'Mobiltelefon' in address else '',
                       'Telephone': address['Telefon'] if 'Telefon' in address else '',
                       'Responsible': manager['uuid'],
                       'Company': employee['Org-enhed UUID']
                       }

                rows_persons.append(row)

    mh._write_csv(fieldnames_persons, rows_persons, filename_persons)
    mh._write_csv(fieldnames_org, rows_org, filename_org)


def find_org_manager(mh, node):

    new_manager = mh.read_organisation_managers(node.name, True)
    if new_manager:
        return new_manager
    elif node.depth != 0:
        return mh.read_organisation_managers(node.parent.name, True)
    else:
        return {'uuid': ''}


def export_planorama_org(mh, nodes, filename):
    fieldnames = ['Root', 'Number', 'Name']
    rows = []
    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        # Do not add "Afdelings-niveau"
        if ou['org_unit_type']['name'] != 'Afdelings-niveau':
            fra = ou['validity']['from'] if ou['validity']['from'] else ''
            til = ou['validity']['to'] if ou['validity']['to'] else ''
            over_uuid = ou['parent']['uuid'] if ou['parent'] else ''
            row = {'Root': over_uuid,
                   'Number': ou['uuid'],
                   'Name': ou['name']}
            rows.append(row)

    mh._write_csv(fieldnames, rows, filename)


if __name__ == '__main__':
    export_from_mo(MO_ROOT_UUID, False, MORA_BASE)
