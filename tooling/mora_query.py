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

import time
import queue
import threading
from anytree import PreOrderIter
from mora_helpers import MoraHelper


def export_all_employees(mh, nodes, filename):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    :param nodes: The nodes of the OU tree
    """
    fieldnames = ['CPR-Nummer', 'Ansættelse gyldig fra',
                  'Ansættelse gyldig til', 'Fornavn', 'Efternavn',
                  'Person UUID', 'Brugernavn', 'Org-enhed',
                  'Org-enhed UUID', 'E-mail', 'Telefon',
                  'Stillingsbetegnelse', 'Engagement UUID']
    rows = []
    for node in PreOrderIter(nodes['root']):
        employees = mh.read_organisation_people(node.name)
        for uuid, employee in employees.items():
            row = {}
            address = mh.read_user_address(uuid, username=True, cpr=True)
            row.update(address)  # E-mail, Telefon
            row.update(employee)  # Everything else
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_all_teams(mh, nodes, filename):
    """ Traverses a tree of OUs, for each OU finds associations
    :param nodes: The nodes of the OU tree
    """
    fieldnames = ['Org-UUID', 'Org-enhed', 'Overordnet UUID', 'Navn',
                  'Person UUID', 'CPR-Nummer']
    rows = []
    for node in PreOrderIter(nodes['root']):
        people = mh.read_organisation_people(node.name, 'association',
                                             split_name=False)
        for uuid, person in people.items():
            ou = mh.read_organisationsenhed(node.name)
            row = {}
            row['Org-UUID'] = ou['uuid']
            row['Overordnet UUID'] = ou['parent']['uuid']
            address = mh.read_user_address(uuid, cpr=True)
            row.update(address)  # E-mail, Telefon
            row.update(person)  # Everything else
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_adm_org(mh, nodes, filename):
    fieldnames = ['uuid', 'Navn', 'Enhedtype UUID',
                  'Gyldig fra', 'Gyldig til', 'Enhedstype Titel']
    rows = []
    for node in PreOrderIter(nodes['root']):
        ou = mh.read_organisationsenhed(node.name)
        fra = ou['validity']['from'] if ou['validity']['from'] else ''
        til = ou['validity']['to'] if ou['validity']['to'] else ''
        over_uuid = ou['parent']['uuid'] if ou['parent'] else ''
        row = {'uuid': ou['uuid'],
               'Overordnet ID': over_uuid,
               'Navn': ou['name'],
               'Enhedtype UUID': ou['org_unit_type']['uuid'],
               'Gyldig fra': fra,
               'Gyldig til': til,
               'Enhedstype Titel': ou['org_unit_type']['name']}
        rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_managers(mh, nodes, filename):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    The list of managers will be saved o a csv-file.
    :param nodes: The nodes of the OU tree
    """
    fieldnames = mh._create_fieldnames(nodes)
    fieldnames += ['Ansvar', 'Navn', 'Telefon', 'E-mail']
    rows = []
    for node in PreOrderIter(nodes['root']):
        manager = mh.read_organisation_managers(node.name)
        if manager:
            row = {}
            path_dict = mh._create_path_dict(fieldnames, node)
            address = mh.read_user_address(manager['uuid'])
            row.update(path_dict)  # Path
            row.update(manager)    # Navn, Ansvar
            row.update(address)    # E-mail, Telefon
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_orgs(mh, nodes, filename, include_employees=True):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    The list of managers will be saved o a csv-file.
    :param mh: Instance of MoraHelper to do the actual work
    :param nodes: The nodes of the OU tree
    """
    fieldnames = mh._create_fieldnames(nodes)
    if include_employees:
        fieldnames += ['Navn', 'Brugernavn', 'Telefon',
                       'E-mail', 'Adresse']
    else:
        fieldnames += ['Adresse', 'p-nummer']

    rows = []
    for node in PreOrderIter(nodes['root']):
        path_dict = mh._create_path_dict(fieldnames, node)
        org_address = mh.read_ou_address(node.name)
        if include_employees:
            employees = mh.read_organisation_people(node.name,
                                                    split_name=False)
            for uuid, employee in employees.items():
                row = {}
                address = mh.read_user_address(uuid, username=True)
                row.update(path_dict)    # Path
                row.update(address)      # E-mail, Telefon
                row.update(org_address)  # Work address
                row.update(employee)     # Everything else
                rows.append(row)
        else:
            row = {}
            row.update(org_address)  # Work address
            row.update(path_dict)  # Path
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def export_udvalg(mh, nodes, filename, fieldnames, org_types):
    """ Traverses a tree of OUs, find members of 'udvalg'
    :param mh: Instance of MoraHelper to do the actual work
    :param nodes: The nodes of the OU tree
    :param fieldnames: Fieldnames for structur of the 'udvalg'
    :param org_types: Org types belong to this kind of 'udvalg'
    """
    fieldnames = fieldnames + ['Fornavn', 'Efternavn', 'Brugernavn', 'Post',
                               'Leder']
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
            row.update(path_dict)    # Path
            row.update(address)      # Brugernavn
            row.update(employee)     # Everything else
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def cache_user(mh, user_queue):
    """ Read all employees in organisation and save the queries in the
    MoraHelper instance.
    :param mh: Instance of MoraHelpers
    :param: user_queue: Queue with all users
    """
    while not user_queue.empty():
        user = user_queue.get_nowait()
        mh.read_user_address(user['uuid'], username=True)
        user_queue.task_done()


def pre_cache_users(mh):
    """ Pre-read all users in organisation, can give a significant
    performance enhancement, since this can be multi-threaded. Only
    works for the complete organisation.
    """
    org_id = mh.read_organisation()
    user_queue = queue.Queue()
    for user in mh._mo_lookup(org_id, 'o/{}/e?limit=99999')['items']:
        user_queue.put(user)
    workers = {}
    for i in range(0, 5):
        workers[i] = threading.Thread(target=cache_user,
                                      args=[mh, user_queue])
        workers[i].start()
    user_queue.join()


if __name__ == '__main__':
    threaded_speedup = False
    t = time.time()

    mh = MoraHelper()

    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    for root in roots:
        if root['name'] == 'Ballerup Kommune':
            ballerup = root['uuid']
        if root['name'] == '9B':
            sd = root['uuid']
        if root['name'] == 'Hoved-MED':
            udvalg = root['uuid']

    if threaded_speedup:
        pre_cache_users(mh)
        print('Build cache: {}'.format(time.time() - t))

    """
    nodes = mh.read_ou_tree(ballerup)
    print('Read nodes: {}s'.format(time.time() - t))

    filename = 'Alle_lederfunktioner_os2mo.csv'
    export_managers(mh, nodes, filename)
    print('Alle ledere: {}s'.format(time.time() - t))

    filename = 'AlleBK-stilling-email_os2mo.csv'
    export_all_employees(mh, nodes, filename)
    print('AlleBK-stilling-email: {}s'.format(time.time() - t))

    filename = 'Ballerup_org_incl-medarbejdere_os2mo.csv'
    export_orgs(mh, nodes, filename)
    print('Ballerup org incl medarbejdere: {}s'.format(time.time() - t))

    filename = 'Adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv'
    export_adm_org(mh, nodes, filename)
    print('Adm-org-incl-start-stop: {}s'.format(time.time() - t))

    filename = 'teams-tilknyttede-os2mo.csv'
    export_all_teams(mh, nodes, filename)
    print('Teams: {}s'.format(time.time() - t))

    nodes = mh.read_ou_tree(sd)
    filename = 'SD-løn org med Pnr_os2mo.csv'
    export_orgs(mh, nodes, filename, include_employees=False)
    print('SD-løn: {}'.format(time.time() - t))
    """

    fieldnames = ['Hoved-MED', 'Center-MED', 'Lokal-MED']
    org_types = ['H-MED', 'C-MED', 'L-MED']
    nodes = mh.read_ou_tree(udvalg)

    #for node in PreOrderIter(nodes['root']):
    #    path_dict = mh._create_path_dict(fieldnames, node, org_types)

    # filename = 'MED-udvalgsmedlemer_i_hieraki.csv'
    # export_orgs(mh, nodes, filename, include_employees=False)
    # print('SD-løn: {}'.format(time.time() - t))

    filename = 'AMR-udvalgsmedlemer_i_hieraki.csv'
    fieldnames = ['Hoved-MED', 'Center-MED', 'Lokal-MED', 'AMR-Gruppe']
    org_types = ['AMR']
    export_udvalg(mh, nodes, filename, fieldnames, org_types)
    print('AMR: {}'.format(time.time() - t))
