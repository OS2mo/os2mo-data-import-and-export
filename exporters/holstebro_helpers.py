# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Holstebro specific export functions, decorators and
helper classes
"""
import csv
import json
import logging
import os
import pathlib
import queue
import threading
import time
from datetime import datetime, timedelta

import requests
from anytree import Node, PreOrderIter

from os2mo_helpers.mora_helpers import MoraHelper

SAML_TOKEN = os.environ.get('SAML_TOKEN', None)

logger = logging.getLogger('holstebro-helpers')


cfg_file = pathlib.Path.cwd() / 'settings' / 'holstebro.settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())


def holstebro_generic_export(mh, nodes, filename):

    fieldnames = ['uuid',
                  'name',
                  'email',
                  'sam_account',
                  'phone',
                  'location',
                  'mobile',
                  'loen_nummer',
                  'forvaltning',  # direktørområde
                  'omraade',  # chefområde
                  'afdeling',  # nærmeste leders afdeling
                  'gruppe',  # engagmentsafdeling
                  'org_sti',
                  'stilling',
                  'engagements_type']

    rows = []

    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        # Do not add "Afdelings-niveau"
        if ou['org_unit_level']['name'] != 'Afdelings-niveau':
            employees = mh.read_organisation_people(node.name, 'engagement', False)
            manager = find_org_manager(mh, node)

            org_path = f"{ou['location']}/{ou['name']}"

            for uuid, employee in employees.items():
                row = {}
                address = mh.read_user_address(uuid, username=True, cpr=True)

                """
                Create generic mapping function from employee data to fieldnames
                """


def export_to_intranote(mh, all_nodes, filename):
    fieldnames = ['NY7-niveau',
                  'NY6-niveau',
                  'NY5-niveau',
                  'NY4-niveau',
                  'NY3-niveau',
                  'NY2-niveau',
                  'NY1-niveau',
                  'Afdelings-niveau',
                  'Navn (for-/efternavn)',
                  'Tjenestenummer',
                  'CPR-nummer',
                  'Ansættelsesdato',
                  'Stamafdeling',
                  'Stilling']

    rows = []

    roots = SETTINGS['exports.holstebro.roots']

    for root_uuid, nodes in all_nodes.items():

        export_all = True if len(roots[root_uuid]) == 0 else False

        for node in PreOrderIter(nodes['root']):
            # if this is only partial export of org and this org unit is not on the exports list
            # or is a child of an org unit in the exports list, continue
            export_node = False
            node_parent = node
            while(node_parent != None and export_node is False):
                export_node = True if node_parent.name in roots[root_uuid] else False
                node_parent = node_parent.parent  # look backwards in hierarchy

            if not export_all and not export_node:
                continue

            ou = mh.read_ou(node.name)

            # Do not add "Afdelings-niveau"
            if ou['org_unit_level']['name'] != 'Afdelings-niveau':

                employees = mh.read_organisation_people(
                    node.name, 'engagement', False)

                ny_levels = {'NY7-niveau': 'Samling',
                             'NY6-niveau': 'Samling',
                             'NY5-niveau': 'Samling',
                             'NY4-niveau': 'Samling',
                             'NY3-niveau': 'Samling',
                             'NY2-niveau': 'Samling',
                             'NY1-niveau': 'Samling',
                             'Afdelings-niveau': ou['name'],
                             'Stamafdeling': ou['name']}

                ou_parent = ou
                while(ou_parent is not None):
                    ny_levels[ou_parent['org_unit_level']
                              ['name']] = ou_parent['name']
                    ou_parent = ou_parent['parent']

                for uuid, employee in employees.items():
                    row = {}
                    row.update(ny_levels)

                    address = mh.read_user_address(uuid, username=True, cpr=True)
                    cut_dates = mh.find_cut_dates(uuid)

                    row.update(
                        {'Navn (for-/efternavn)': employee['Navn'],
                         'Tjenestenummer': employee['User Key'],
                         'CPR-nummer': address['CPR-Nummer'],
                         # dd.mm.yyyy
                         'Ansættelsesdato': cut_dates[0].strftime('%d.%m.%Y'),
                         'Stilling': employee['Titel'] if employee['Titel'] is not None else employee['Stillingsbetegnelse']}
                    )

                    rows.append(row)

    my_options = {"extrasaction": "ignore",
                  "delimiter": ";",
                  "quoting": csv.QUOTE_MINIMAL
                  }

    logger.info(f"Writing {len(rows)} rows to {filename}")
    mh._write_csv(fieldnames, rows, filename, **my_options)


def export_to_essenslms(mh, all_nodes, filename):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    : param nodes: The nodes of the OU tree
    """
    fieldnames = ['name',
                  'handle',
                  'email',
                  'ou_roles',
                  'user_roles',
                  'locale',
                  'tmp_password',
                  'password']

    rows = []

    roots = SETTINGS['exports.holstebro.roots']

    for root_uuid, nodes in all_nodes.items():

        export_all = True if len(roots[root_uuid]) == 0 else False

        for node in PreOrderIter(nodes['root']):
            # if this is only partial export of org and this org unit is not on the exports list
            # or is a child of an org unit in the exports list, continue
            export_node = False
            node_parent = node
            while(node_parent != None and export_node is False):
                export_node = True if node_parent.name in roots[root_uuid] else False
                node_parent = node_parent.parent  # look backwards in hierarchy

            if not export_all and not export_node:
                continue

            ou = mh.read_ou(node.name)

            # Do not add "Afdelings-niveau"
            if ou['org_unit_level']['name'] != 'Afdelings-niveau':

                employees = mh.read_organisation_people(
                    node.name, 'engagement', False)
                manager = find_org_manager(mh, node)

                org_path = f"/Root/MOCH/Holstebro/Holstebro/{ou['location']}/{ou['name']}"

                logger.info(f"Exporting {org_path} to EssensLMS")

                for uuid, employee in employees.items():
                    row = {}
                    address = mh.read_user_address(uuid, username=True, cpr=True)

                    if uuid == manager['uuid']:  # this is the manager
                        # Strip " led-adm" from path, since manager for "led-adm" is also manager for parent level
                        replace_value = f" {SETTINGS['imports.holstebro.leaders.common_management_name']}"
                        org_path = org_path.replace(replace_value, '', 1)
                        ou_role = f"{org_path}:Manager"
                    else:
                        ou_role = f"{org_path}:User"

                    row = {'name': employee['Navn'],
                           'handle': f"{employee['User Key']}".lstrip('0'),
                           'email': address['E-mail'] if 'E-mail' in address else '',
                           'ou_roles': ou_role.replace("\\", "/"),
                           'user_roles': 'Holstebro',
                           'locale': 'da',
                           'tmp_password': 'true',
                           'password': 'abcd1234'
                           }

                    rows.append(row)

    my_options = {"extrasaction": "ignore",
                  "delimiter": ",",
                  "quoting": csv.QUOTE_MINIMAL
                  }

    logger.info(f"Writing {len(rows)} rows to {filename}")
    mh._write_csv(fieldnames, rows, filename, **my_options)


def export_to_planorama(mh, all_nodes, filename_org, filename_persons, org_root_uuid):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    : param nodes: The nodes of the OU tree
    """
    fieldnames_persons = ['integrationsid',
                          'Username',
                          'Name',
                          'Title',
                          'Address',
                          'Email',
                          'Mobile',
                          'Telephone',
                          'Responsible',
                          'Company']
    fieldnames_org = ['Root', 'Number', 'Name', 'Manager']

    rows_org = []
    rows_persons = []

    roots = SETTINGS['exports.holstebro.roots']
    org_root_name = SETTINGS['municipality.name']
    # insert root
    row_org = {'Root': '', 'Number': org_root_uuid, 'Name': org_root_name}
    rows_org.append(row_org)
    logger.info(f"Exporting root unit: {org_root_name} to Planorama")

    for root_uuid, nodes in all_nodes.items():

        # is this a partail export
        export_all = True if len(roots[root_uuid]) == 0 else False

        if not export_all:
            # partial export
            # insert this root_uuid in rows_org with holstebro_uuid as parent
            t_node = Node(root_uuid)
            ou = mh.read_ou(root_uuid)

            # find this unit's manager
            manager = find_org_manager(mh, t_node)

            row_org = {'Root': org_root_uuid,
                       'Number': ou['uuid'],
                       'Name': ou['name'],
                       'Manager': manager['uuid']}

            rows_org.append(row_org)
            logger.info(f"Exporting {ou['name']} to Planorama")

        for node in PreOrderIter(nodes['root']):
            # if this is only partial export of org and this org unit is not on the exports list
            # or is a child of an org unit in the exports list, continue
            export_node = False
            node_parent = node
            while(node_parent != None and export_node is False):
                export_node = True if node_parent.name in roots[root_uuid] else False
                node_parent = node_parent.parent  # look backwards in hierarchy

            if not export_all and not export_node:
                continue

            ou = mh.read_ou(node.name)
            # Do not add "Afdelings-niveau"
            if ou['org_unit_level']['name'] != 'Afdelings-niveau':

                # find this unit's manager
                manager = find_org_manager(mh, node)

                if(manager['uuid'] != ''):
                    manager_engagement = mh.read_user_engagement(manager['uuid'])
                else:
                    manager_engagement = [{'user_key': ''}]

                if not export_all:  # and ou['uuid'] in root[root_uuid]
                    # partial export
                    # add this ou directly under the root_uuid thats being exported
                    row_org = {'Root': root_uuid, 'Number': ou['uuid'],
                               'Name': ou['name'], 'Manager': manager['uuid']}
                else:
                    parent_ou_uuid = ou['parent']['uuid'] if ou['parent'] else org_root_uuid
                    row_org = {'Root': parent_ou_uuid, 'Number': ou['uuid'],
                               'Name': ou['name'], 'Manager': manager['uuid']}

                rows_org.append(row_org)
                logger.info(f"Exporting {ou['name']} to Planorama")

                # Read all employees in org unit and add them to employee export rows
                employees = mh.read_organisation_people(
                    node.name, 'engagement', False)
                for uuid, employee in employees.items():
                    row = {}
                    address = mh.read_user_address(uuid, username=True, cpr=True)

                    # is this employee the manager for the department? Then fetch the parent ou's manager
                    if uuid != manager['uuid']:
                        responsible = manager_engagement[0]['user_key']
                    else:
                        parent_manager = find_org_manager(mh, node.parent)
                        if(parent_manager['uuid'] != ''):
                            pmanager_engagement = mh.read_user_engagement(
                                parent_manager['uuid'])
                        else:
                            pmanager_engagement = [{'user_key': ''}]

                        responsible = pmanager_engagement[0]['user_key']

                    row = {'integrationsid': uuid,
                           'Username': f"HK-{employee['User Key']}",
                           'Name': employee['Navn'],
                           'Title': employee['Titel'] if employee['Titel'] is not None else employee['Stillingsbetegnelse'],
                           'Address': address['Lokation'] if 'Lokation' in address else '',
                           'Email': address['E-mail'] if 'E-mail' in address else '',
                           'Mobile': address['Mobiltelefon'] if 'Mobiltelefon' in address else '',
                           'Telephone': address['Telefon'] if 'Telefon' in address else '',
                           'Responsible': f"HK-{responsible}" if responsible != '' else '',
                           'Company': employee['Org-enhed UUID']
                           }

                    rows_persons.append(row)

    logger.info(f"Writing {len(rows_persons)} rows to {filename_persons}")
    mh._write_csv(fieldnames_persons, rows_persons, filename_persons)

    logger.info(f"Writing {len(rows_org)} rows to {filename_org}")
    mh._write_csv(fieldnames_org, rows_org, filename_org)


def update_org_with_hk_managers(mh, nodes):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    : param nodes: The nodes of the OU tree
    """
    managerHelper = HolstebroHelper(mh)

    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        # for each ou, check if name contains _leder
        # if so, check ou for "tilknytninger" and set  this as leader for ou.parent
        if ou['org_unit_level']['name'] == 'Afdelings-niveau' and ou['name'].count(SETTINGS['imports.holstebro.leaders.manager_extension']) == 1:
            # We have an Afdeling with name _leder, find associated employees and make them leaders in the parent ou
            associated_employees = mh.read_organisation_people(
                node.name, 'association', False)

            if(ou['parent'] != None and len(associated_employees) == 1):
                manager_uuid = list(associated_employees)[0]
                managerHelper.update_manager(
                    ou['parent'], associated_employees[manager_uuid])

            # This manager is now manager for parent ou
            # if parent ou's name ends with "led-adm", make employee
            # manager for the parent ou's parent as well.
            if ou['parent']['parent'] != None and ou['parent']['name'].count(SETTINGS['imports.holstebro.leaders.common_management_name']) == 1:
                manager_uuid = list(associated_employees)[0]
                managerHelper.update_manager(
                    ou['parent']['parent'], associated_employees[manager_uuid])


def find_org_manager(mh, node):
    """
    Searches for a manager for the given node.
    Returns the manager directly associated with the node
    OR the enherited manager from higherlevel
    If no managers at all in organisation, the empty
    dict is returned
    """

    if node == None:
        return {'uuid': ''}

    new_manager = mh.read_ou_manager(node.name, True)

    if new_manager:
        return new_manager
    elif node.depth != 0:
        return find_org_manager(mh, node.parent)
    else:
        return {'uuid': ''}


class HolstebroHelper(object):
    def __init__(self, mh):
        self.mh = mh
        self.manager_info = self._get_manager_types()

    def _get_manager_types(self):
        org_uuid = self.mh.read_organisation()
        manager_types = self.mh._mo_lookup(org_uuid, 'o/{}/f/manager_type')
        manager_levels = self.mh._mo_lookup(org_uuid, 'o/{}/f/manager_level')
        manager_responsibility = self.mh._mo_lookup(
            org_uuid, 'o/{}/f/responsibility')

        return_dict = {'manager_types': {},
                       'manager_levels': {},
                       'manager_responsibility': {}}

        for mt in manager_types['data']['items']:
            return_dict['manager_types'].update({mt['name']: mt})

        for ml in manager_levels['data']['items']:
            return_dict['manager_levels'].update({ml['name']: ml})

        for mr in manager_levels['data']['items']:
            return_dict['manager_responsibility'].update({mr['uuid']: mr})
        # TODO: Add check for SETTINGS['imports.holstebro.leaders.responsibility'] in responsibility

        return return_dict

    def _get_org_level(self, ou):
        if ou['org_unit_level']['name'] == 'NY5-niveau':
            return self.manager_info['manager_levels']['Direktør']['uuid']

        elif ou['org_unit_level']['name'] == 'NY4-niveau':
            return self.manager_info['manager_levels']['Chef']['uuid']

        elif ou['org_unit_level']['name'] == 'NY3-niveau':
            return self.manager_info['manager_levels']['Leder']['uuid']

        else:
            return self.manager_info['manager_levels']['Leder']['uuid']

    def _create_manager(self, ou_uuid, person_uuid, level_uuid):

        payload = {
            "type": "manager",
            "org_unit": {
                "uuid": ou_uuid
            },
            "address": [],
            "manager_level": {
                "uuid": level_uuid
            },
            "manager_type": {
                "uuid": self.manager_info['manager_types']['Leder']['uuid']
            },
            "person": {
                "uuid": person_uuid

            },
            "responsibility": [
                {
                    "uuid": SETTINGS['imports.holstebro.leaders.responsibility']
                }
            ],
            "user_key": "HK-AUTO-ASSIGNED",
            "validity": {
                "from": self._get_date()
            }
        }
        logger.info(
            "Attempting to create manager for ou uuid: {}".format(ou_uuid))
        self.mh._mo_post('details/create', payload, False)
        logger.info("Manager created")

    def _terminate_manager(self, manager_releation_uuid):
        payload = {
            'type': 'manager',
            'uuid': manager_releation_uuid,
            'validity': {
                    'to': self._get_date(1)
            }
        }

        logger.info(
            "Attempting to terminate manager relation uuid: {}".format(manager_releation_uuid))
        self.mh._mo_post('details/terminate', payload, True)
        logger.info("Manager terminated")

    def _get_date(self, td=0):
        rdate = datetime.today() - timedelta(days=td)
        return rdate.strftime('%Y-%m-%d')

    def update_manager(self, ou, manager):
        """Checks if manager is manger for the given ou
        Update the ou with new manager and deletes ones found
        """

        ou_uuid = ou['uuid']
        manager_uuid = manager['Person UUID']

        # First check that the new manager has active engagements
        manager_engagements = self.mh.read_user_engagement(manager_uuid)

        has_engagements = True if len(manager_engagements) > 0 else False

        # Create list of managers from this Afdelings-niveau
        # update parent ou
        # Get non-inherited manager, ALWAYS returns 1 or no manager
        ou_manager = self.mh.read_ou_manager(ou_uuid, False)

        if ou_manager == {}:  # no manager, create it
            logger.info("Manager for {} should be {}".format(
                ou['name'], manager['Navn']))

            manager_level = self._get_org_level(ou)
            if has_engagements:
                self._create_manager(ou_uuid, manager_uuid, manager_level)
            else:
                logger.error(
                    f"Manager with uuid: {manager_uuid} has no active engagements and will not be made manager")

        elif ou_manager['uuid'] != manager_uuid:
            logger.info("Manager for {} should be {}".format(
                ou['name'], manager['Navn']))

            self._terminate_manager(ou_manager['relation_uuid'])

            manager_level = self._get_org_level(ou)
            if has_engagements:
                self._create_manager(ou_uuid, manager_uuid, manager_level)
            else:
                logger.error(
                    f"Manager with uuid: {manager_uuid} has no active engagements and will not be made manager")

        if ou_manager != {} and ou_manager['uuid'] == manager_uuid:
            # current manager is this ou's manager,
            # but the manager no longer has active engagements
            # then remove manager
            if not has_engagements:
                logger.info("Manager for {} is {} and has no engagements. Will be terminated.".format(
                    ou['name'], manager['Navn']))
                self._terminate_manager(ou_manager['relation_uuid'])

    def add_employee(self, employee_info):
        # TODO: add check for data before posting

        engagement_types = {
            "ML": "55e2f6c9-2dcb-cdc1-556c-d78d7f9e173d",
            "TL": "b523b991-ece7-0cce-dbcd-7f227e57ad81"
        }

        org_uuid = self.mh.read_organisation()

        employee_info['org']['uuid'] = org_uuid

        response = self.mh._mo_post('e/create', employee_info, False)
        if response.status_code == 400:
            error = response.json()['description']
            raise requests.HTTPError("Inserting mora data failed")
        elif response.status_code not in (200, 201):
            logger.error(
                'MO post. Response: {}, data'.format(response.text, response)
            )
            raise requests.HTTPError("Inserting mora data failed")
        else:
            uuid = response.json()
        return uuid
