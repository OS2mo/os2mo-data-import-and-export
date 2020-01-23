#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Helper class to make a number of pre-defined queries into MO
"""
import os
import time
from anytree import PreOrderIter
from os2mo_helpers.mora_helpers import MoraHelper
import common_queries as cq

MORA_BASE = os.environ.get('MORA_BASE')


def export_udvalg(mh, nodes, filename, fieldnames, org_types):
    """ Traverses a tree of OUs, find members of 'udvalg'
    :param mh: Instance of MoraHelper to do the actual work
    :param nodes: The nodes of the OU tree
    :param fieldnames: Fieldnames for structur of the 'udvalg'
    :param org_types: Org types belong to this kind of 'udvalg'
    """
    fieldnames = fieldnames + ['Fornavn', 'Efternavn', 'Brugernavn', 'Post',
                               'Leder', 'Tillidrepræsentant', 'E-mail', 'Telefon']
    rows = []
    for node in PreOrderIter(nodes['root']):
        path_dict = mh._create_path_dict(fieldnames, node, org_types)
        if not path_dict:
            continue
        employees = mh.read_organisation_people(node.name,
                                                person_type='association',
                                                split_name=True)
        for uuid, employee in employees.items():
            row = {}
            address = mh.read_user_address(uuid, username=True)
            mh.read_user_manager_status(uuid)
            if mh.read_user_manager_status(uuid):
                row['Leder'] = 'Ja'
            if 'Tillidrepræsentant' in mh.read_user_roller(uuid):
                row[' Tillidrepræsentant'] = 'Ja'
            row.update(path_dict)    # Path
            row.update(address)      # Brugernavn
            row.update(employee)     # Everything else
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


if __name__ == '__main__':
    threaded_speedup = False
    t = time.time()

    mh = MoraHelper(hostname=MORA_BASE, export_ansi=False)

    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    print(roots)
    for root in roots:
        if root['name'] == 'Ballerup Kommune':
            ballerup = root['uuid']
        if root['name'] == '9B':
            sd = root['uuid']
        if root['name'] == 'H-MED Hoved-MED':
            udvalg = root['uuid']

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    nodes = mh.read_ou_tree(ballerup)
    print('Read nodes: {}s'.format(time.time() - t))

    filename = 'Alle_lederfunktioner_os2mo.csv'
    cq.export_managers(mh, nodes, filename)
    print('Alle ledere: {}s'.format(time.time() - t))
    exit()
    filename = 'AlleBK-stilling-email_os2mo.csv'
    cq.export_all_employees(mh, nodes, filename)
    print('AlleBK-stilling-email: {}s'.format(time.time() - t))

    filename = 'Ballerup_org_incl-medarbejdere_os2mo.csv'
    cq.export_orgs(mh, nodes, filename)
    print('Ballerup org incl medarbejdere: {}s'.format(time.time() - t))

    filename = 'Adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv'
    cq.export_adm_org(mh, nodes, filename)
    print('Adm-org-incl-start-stop: {}s'.format(time.time() - t))

    filename = 'teams-tilknyttede-os2mo.csv'
    cq.export_all_teams(mh, nodes, filename)
    print('Teams: {}s'.format(time.time() - t))

    nodes = mh.read_ou_tree(sd)
    filename = 'SD-løn org med Pnr_os2mo.csv'
    cq.export_orgs(mh, nodes, filename, include_employees=False)
    print('SD-løn: {}'.format(time.time() - t))

    nodes = mh.read_ou_tree(udvalg)
    filename = 'AMR-udvalgsmedlemer_i_hieraki.csv'
    fieldnames = ['Hoved-MED', 'Center-MED', 'Lokal-MED', 'AMR-Gruppe']
    org_types = ['AMR']
    export_udvalg(mh, nodes, filename, fieldnames, org_types)
    print('AMR: {}'.format(time.time() - t))

    filename = 'MED-udvalgsmedlemer_i_hieraki.csv'
    fieldnames = ['Hoved-MED', 'Center-MED', 'Lokal-MED', 'AMR-Gruppe']
    org_types = ['H-MED', 'C-MED', 'L-MED']
    export_udvalg(mh, nodes, filename, fieldnames, org_types)
    print('MED: {}'.format(time.time() - t))

    print('Export completed')
