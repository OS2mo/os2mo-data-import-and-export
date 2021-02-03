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
SETTINGS = json.loads(cfg_file.read_text(encoding='utf-8'))


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


def export_to_orgviewer(mh, all_nodes):

    # Root must be Holstebro Kommune for org viewer
    roots = SETTINGS['exports.holstebro.roots']
    org_root_name = SETTINGS['municipality.name']
    org_root_uuid = SETTINGS['municipality.uuid']

    data_fields = {'name': 'Location', 'value': '', 'type': 'text'}
    people = []
    assignments = []

    # Manually add root org unit "Holstebro Kommune" from settings and
    # add manager to assignments and people
    data = {'api_version': '1.0',
            'chart': {
                'id': org_root_uuid,
                'name': org_root_name,
                'description': '',
                'parent_id': '',
                'manager_id': SETTINGS['exports.holstebro.root_manager'],
                'staff_department': 'N',
                'dataFields': [data_fields],
                'children': [],
                "showChildren": True
            },
            'people': people,
            'assignments': assignments
            }

    # this is where we start adding children
    # Create dict with {'parent_uuid': ref to children[]}
    children_ref = {}
    children_ref.update({org_root_uuid: data['chart']['children']})

    logger.info(f"Exporting root unit: {org_root_name} to Org. viewer")

    # is this a partial exports
    export_all = True if len(roots[org_root_uuid]) == 0 else False

    logger.info(f"Exporting {org_root_name} to Org. viewer")

    nodes = all_nodes[org_root_uuid]

    # First create list of all children
    # then put them in hierarchial order

    for node in PreOrderIter(nodes['root']):
        # if this is only partial export of org and this org unit is not on the exports list
        # or is a child of an org unit in the exports list, continue
        export_node = False
        node_parent = node
        while(node_parent != None and export_node is False):
            export_node = True if node_parent.name in roots[org_root_uuid] else False
            node_parent = node_parent.parent  # look backwards in hierarchy

        if not export_all and not export_node:
            continue

        ou = mh.read_ou(node.name)

        if ou['org_unit_level']['name'] == 'Afdelings-niveau':
            continue

        # find this unit's manager
        manager = find_org_manager(mh, node)

        # figure out what the parent ou should be
        # If this is only a partial export org, then add the ou in roots directly under
        # root ou.

        # Default is root uuid
        parent_ou_uuid = org_root_uuid
        if export_all is True:
            parent_ou_uuid = ou['parent']['uuid'] if ou['parent'] else org_root_uuid
        elif export_node is True and ou['uuid'] not in roots[org_root_uuid]:
            parent_ou_uuid = ou['parent']['uuid'] if ou['parent'] else org_root_uuid

        if ou['uuid'] != org_root_uuid:
            # Do no insert root ou. This has already been added
            child = {
                'id': ou['uuid'],
                'name': ou['name'],
                'description': '',
                'parent_id': parent_ou_uuid,
                'staff_department': 'N',
                'manager_id': manager['uuid'],
                'dataFields': [data_fields],
                'children': [],
                'showChildren': False
            }

            logger.info(
                f"Exporting {ou['name']} to Org. viewer with parent {parent_ou_uuid}")

            # add child to parent ous list of children
            children_ref[parent_ou_uuid].append(child)

            # add uuid for this ou to children reference dict
            # and point to this ou's list of children for later addition
            children_ref.update({ou['uuid']: child['children']})

        # Read all employees in org unit and add them to employee export rows
        employees = mh.read_organisation_people(
            node.name, "engagement", False)
        for uuid, employee in employees.items():
            address = mh.read_user_address(uuid, username=True, cpr=False)

            # is this employee the manager for the department? Then fetch the parent ou's manager
            if uuid != manager['uuid']:
                responsible = manager['uuid']
            else:
                parent_manager = find_org_manager(mh, node.parent)
                responsible = parent_manager['uuid']

            person = {
                'id': uuid,
                'name': employee['Navn'],
                'photo': 'default.png',
                'main_role': employee['Titel'] if employee['Titel'] is not None else employee['Stillingsbetegnelse'],
                'function': ''
            }

            assignment = {
                "department_id": employee['Org-enhed UUID'],
                "person_id": uuid,
                "role": person['main_role'],
                "id": employee['Engagement UUID']
            }

            people.append(person)
            assignments.append(assignment)

    # add pre and postfix to json file
    js_prefix = "var INPUT_DATA="
    js_postfix = ";var UPDATED_ON=\"" + datetime.now().strftime("%d-%m-%Y") + "\""
    filename = SETTINGS['exports.holstebro.org_viewer.filename']

    with open(filename, encoding='utf-8', mode='w') as outfile:
        outfile.write(js_prefix)
        json.dump(data, outfile, indent=4, separators=(
            ',', ': '), ensure_ascii=False)
        outfile.write(js_postfix)
        outfile.close()


def export_orgs(mh, nodes, filename):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    The list of managers will be saved o a csv-file.
    :param mh: Instance of MoraHelper to do the actual work
    :param nodes: The nodes of the OU tree
    """
    fieldnames = mh._create_fieldnames(nodes)
    fieldnames += ['Leder']

    rows = []
    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)

        # Do not add "Afdelings-niveau"
        if ou['org_unit_level']['name'] != 'Afdelings-niveau':
            path_dict = mh._create_path_dict(fieldnames, node)
            # find this unit's manager
            manager = mh.read_ou_manager(ou['uuid'], True)

            row = {}
            row.update({'Leder': manager['Navn']})  # Work address
            row.update(path_dict)  # Path
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


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
                    node.name, 'engagement', False, primary_types=SETTINGS['exports.holstebro.primary_types'])

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

                    handle = f"{employee['User Key']}".lstrip('0')

                    # For essenslms, only add employees with key less than max value from settingsfile
                    # if user key is not a digit, always add it.
                    if not handle.isdigit() or int(handle) < SETTINGS['exports.holstebro.essenslms.max_user_key']:
                        row = {'name': employee['Navn'],
                               'handle': handle,
                               'email': address['E-mail'] if 'E-mail' in address else '',
                               'ou_roles': ou_role.replace("\\", "/"),
                               'user_roles': 'Holstebro',
                               'locale': 'da',
                               'tmp_password': 'true',
                               'password': 'abcd1234'
                               }

                        rows.append(row)
                    else:
                        logger.debug(
                            f"User not exported to essenslms. Uuid and userkey: {uuid}, {handle}")

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
        # if so, check ou for "tilknytninger" and set this as leader for ou.parent
        if _is_leader_unit(ou):
            # We have an Afdeling with name _leder, find associated employees and make them leaders in the parent ou
            associated_employees = mh.read_organisation_people(
                node.name, 'association', False)

            # Find the manager if an employee is associated with manager departement
            manager = {}
            if len(associated_employees) > 0:
                # Take the first employees associated with the ou and make them leader
                manager_uuid = list(associated_employees)[0]
                manager = associated_employees[manager_uuid]

            if len(associated_employees) > 1:
                # This is an error. There should be no more than one employeen in manager unit.
                # Log this as an error.
                logger.error(
                    "More than one manager associated with: {}".format(ou['name']))

            if ou['parent'] != None:
                managerHelper.update_manager(
                    ou['parent'], manager)

            # This manager is now manager for parent ou - or has been removed
            # if parent ou's name ends with "led-adm", make employee
            # manager for the parent ou's parent as well.
            if _is_cm_unit(ou):
                managerHelper.update_manager(
                    ou['parent']['parent'], manager)


def _is_leader_unit(ou):
    is_leader_unit = False

    if ou['org_unit_level']['name'] == 'Afdelings-niveau' and ou['name'].count(SETTINGS['imports.holstebro.leaders.manager_extension']) == 1 and ou['name'].count(SETTINGS['imports.holstebro.leaders.manager_prefix_exclude']) == 0:
        is_leader_unit = True

    return is_leader_unit


def _is_cm_unit(ou):
    is_cm_unit = False

    if ou['parent']['parent'] != None and ou['parent']['name'].count(SETTINGS['imports.holstebro.leaders.common_management_name']) == 1:
        is_cm_unit = True

    return is_cm_unit


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
        self.default_responsibility = self._get_default_responsibility()

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

        for mr in manager_responsibility['data']['items']:
            return_dict['manager_responsibility'].update({mr['uuid']: mr})
        # TODO: Add check for SETTINGS['imports.holstebro.leaders.responsibility'] in responsibility

        return return_dict

    def _get_default_responsibility(self):
        default_responsibility = SETTINGS['imports.holstebro.leaders.responsibility']

        # TODO: check all availresponsibilities against default and assign this
        # for responsibility in self.manager_info['manager_responsibility']:
        #    if responsibility == default_responsibility:
        #        break
        return default_responsibility

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
                    "uuid": self.default_responsibility
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
        manager_engagements = []

        if manager == {}:
            manager_uuid = None
        else:
            manager_uuid = manager['Person UUID']
            manager_engagements = self.mh.read_user_engagement(manager_uuid)

        # First check that the new manager has active engagements
        has_engagements = True if len(manager_engagements) > 0 else False

        # Create list of managers from this Afdelings-niveau
        # update parent ou
        # Get non-inherited manager, ALWAYS returns 1 or no manager
        ou_manager = self.mh.read_ou_manager(ou_uuid, False)

        if manager_uuid != None:
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
        else:
            if ou_manager != {}:
                logger.info("Manager for {} is {} and but no leader association exists. Will be terminated.".format(
                    ou['name'], ou_manager['Navn']))
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
