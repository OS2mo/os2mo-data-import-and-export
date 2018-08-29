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

import csv
import requests
from anytree import Node, PreOrderIter


class MoraQuery(object):
    def __init__(self, hostname='localhost'):
        self.host = 'http://' + hostname + '/service/'
        self.ou_cache = {}
        self.address_cache = {}

    def _split_name(self, name):
        """ Split a name into first and last name.
        Currently just splits at last space, but this might turn out to
        be a too simple rule.
        :param name: The name to split.
        :return: Dict with first and last name separated.
        """
        splitted_name = {'Fornavn': name[:name.rfind(' ')],
                         'Efternavn': name[name.rfind(' '):]}
        return splitted_name

    def _read_node_path(self, node):
        """ Find the full path for a given node
        :param node: The node to find the path for
        :return: List with full path for the node
        """
        path = []
        for sub_node in node.path:
            if sub_node.name in self.ou_cache:
                ou = self.ou_cache[sub_node.name]
            else:
                ou = self.read_organisationsenhed(sub_node.name)
                self.ou_cache[sub_node.name] = ou
            path += [ou['name']]
        return path

    def _create_fieldnames(self, nodes):
        """ Create a list of fieldnames that has a suitable length
        to accomodate the full tree debth. First names are hard-coded
        the rest are auto-generated
        :param nodes: List of all nodes in tree
        :return: A list of fieldnames
        """
        fieldnames = ['root', 'org', 'sub org']
        for i in range(2, nodes['root'].height):
            fieldnames += [str(i) + 'xsub org']
        return fieldnames

    def _write_csv(self, fieldnames, rows, filename):
        with open(filename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames,
                                    extrasaction='ignore')
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _create_path_dict(self, fieldnames, node):
        path = self._read_node_path(node)
        path_dict = {}
        i = 0
        for element in path:
            path_dict[fieldnames[i]] = element
            i += 1
        return path_dict

    def read_organisation(self):
        """ Read the main Organisation, all OU's will have this as root.
        Currently reads only one, theroretically more than root org can exist.
        :return: UUID of root organisation
        """
        url = self.host + 'o'
        response = requests.get(url)
        org_id = response.json()
        return org_id[0]['uuid']

    def read_organisationsenhed(self, uuid):
        """ Return a dict with the data available about an OU
        :param uuid: The UUID of the OU
        :return: Dict with the information about the OU
        """
        url = self.host + 'ou/' + uuid
        response = requests.get(url)
        org_enhed = response.json()
        return org_enhed

    def read_ou_address(self, uuid):
        """ Return a dict with the data available about an OU
        :param uuid: The UUID of the OU
        :return: Dict with the information about the OU
        """
        return_address = {}
        if uuid not in self.address_cache:
            url = self.host + 'ou/' + uuid + '/details/address'
            addresses = requests.get(url).json()
            self.address_cache[uuid] = addresses
        else:
            addresses = self.address_cache[uuid]
        for address in addresses:
            if address['address_type']['scope'] == 'DAR':
                return_address['Adresse'] = address['name']
        return return_address

    def read_user_address(self, user, username=False, cpr=False):
        """ Read phone number and email from user
        :param user: UUID of the wanted user
        :return: Dict witn phone number and email (if the exists in MO
        """
        url = self.host + 'e/' + user + '/details/address'
        addresses = requests.get(url).json()
        return_address = {}
        for address in addresses:
            if address['address_type']['scope'] == 'PHONE':
                return_address['Telefon'] = address['name']
            if address['address_type']['scope'] == 'EMAIL':
                return_address['E-mail'] = address['name']
        if username or cpr:
            url = self.host + 'e/' + user
            personal_info = requests.get(url).json()
            if username:
                return_address['Brugernavn'] = personal_info['user_key']
            if cpr:
                return_address['CPR-Nummer'] = personal_info['cpr_no']
        return return_address

    def read_organisation_managers(self, org_uuid):
        """ Read the manager of an organisation.
        Currently an exception will be raised if an ou has more than
        one manager.
        :param org_uuid: UUID of the OU to find the manager of
        :return: Returns 0 or 1 manager of the OU.
        """
        manager_list = {}
        url = self.host + 'ou/' + org_uuid + '/details/manager'
        managers = requests.get(url).json()
        # Iterate over all managers, use uuid as key, if more than one
        # distinct uuid shows up in list, rasie an error
        for manager in managers:
            uuid = manager['person']['uuid']
            # Note: We pick the first responsibility, no room for more in list
            # TODO: This is not good enough, we need correct responsibility
            data = {'Navn': manager['person']['name'],
                    'Ansvar': manager['responsibility'][0]['name'],
                    'uuid': uuid
                    }
            manager_list[uuid] = data
        if len(manager_list) == 0:
            manager = {}
        elif len(manager_list) == 1:
            manager = manager_list[uuid]
        elif len(manager_list) > 1:
            # Currently we do not support multiple managers
            print(org_uuid)
            manager = manager_list[uuid]
            # TODO: Fix this...
            # raise Exception('Too many managers')
        return manager

    def read_organisation_employees(self, org_uuid, split_name=True):
        """ Read all employees in an ou. If the same employee is listed
        more than once, only the latest listing will be included.
        :param org_uuid: UUID of the OU to find emplyees in.
        :return: The list of emplyoees
        """
        # TODO: Check if managers are included in engament
        employee_list = {}
        url = self.host + 'ou/' + org_uuid + '/details/engagement'
        employees = requests.get(url).json()
        for employee in employees:
            uuid = employee['person']['uuid']
            data = {'Ansættelse gyldig fra': employee['validity']['from'],
                    'Ansættelse gyldig til': employee['validity']['to'],
                    'Person UUID': uuid,
                    'Org-enhed': employee['org_unit']['name'],
                    'Org-enhed UUID': employee['org_unit']['uuid'],
                    'Stillingsbetegnelse': employee['job_function']['name'],
                    'Engagement UUID': employee['uuid']
                    }
            # Finally, add name
            if split_name:
                data.update(self._split_name(employee['person']['name']))
            else:
                data['Navn'] = employee['person']['name']
            employee_list[uuid] = data
        return employee_list

    def read_top_units(self, organisation):
        """ Read the ous tha refers directoly to the organisation
        :param organisation: UUID of the organisation
        :return: List of UUIDs of the root OUs in the organisation
        """
        url = self.host + 'o/' + organisation + '/children'
        response = requests.get(url)
        units = response.json()
        return units

    def read_ou_tree(self, org, nodes={}, parent=None):
        """ Recursively find all sub-ou's beneath current node
        :param org: The organisation to start the tree from
        :param nodes: Dict with all modes in the tree
        :param parent: The parent of the current node, None if this is root
        :return: A dict with all nodes in tree, top node is named 'root'
        """
        url = self.host + 'ou/' + org + '/children'
        units = requests.get(url).json()
        if parent is None:
            nodes['root'] = Node(org)
            parent = nodes['root']
        for unit in units:
            uuid = unit['uuid']
            nodes[uuid] = Node(uuid, parent=parent)
            if unit['child_count'] > 0:
                nodes = self.read_ou_tree(uuid, nodes, nodes[uuid])
        return nodes

    def export_all_employees(self, nodes, filename):
        """ Traverses a tree of OUs, for each OU finds the manager of the OU.
        The list of managers will be saved o a csv-file.
        :param nodes: The nodes of the OU tree
        """
        fieldnames = ['CPR-Nummer', 'Ansættelse gyldig fra',
                      'Ansættelse gyldig til', 'Fornavn', 'Efternavn',
                      'Person UUID', 'Brugernavn', 'Org-enhed',
                      'Org-enhed UUID', 'E-mail', 'Telefon',
                      'Stillingsbetegnelse', 'Engagement UUID']
        rows = []
        for node in PreOrderIter(nodes['root']):
            employees = self.read_organisation_employees(node.name)
            for uuid, employee in employees.items():
                row = {}
                address = self.read_user_address(uuid, username=True, cpr=True)
                row.update(address)  # E-mail, Telefon
                row.update(employee)  # Everything else
                rows.append(row)
        self._write_csv(fieldnames, rows, filename)

    def export_bosses(self, nodes, filename):
        """ Traverses a tree of OUs, for each OU finds the manager of the OU.
        The list of managers will be saved o a csv-file.
        :param nodes: The nodes of the OU tree
        """
        fieldnames = self._create_fieldnames(nodes)
        fieldnames += ['Ansvar', 'Navn', 'Telefon', 'E-mail']
        rows = []
        for node in PreOrderIter(nodes['root']):
            manager = self.read_organisation_managers(node.name)
            if manager:
                row = {}
                path_dict = self._create_path_dict(fieldnames, node)
                address = self.read_user_address(manager['uuid'])
                row.update(path_dict)  # Path
                row.update(manager)    # Navn, Ansvar
                row.update(address)    # E-mail, Telefon
                rows.append(row)
        self._write_csv(fieldnames, rows, filename)

    def export_orgs(self, nodes, filename, include_employees=True):
        """ Traverses a tree of OUs, for each OU finds the manager of the OU.
        The list of managers will be saved o a csv-file.
        :param nodes: The nodes of the OU tree
        """
        fieldnames = self._create_fieldnames(nodes)
        if include_employees:
            fieldnames += ['Navn', 'Brugernavn', 'Telefon',
                           'E-mail', 'Adresse']
        rows = []
        for node in PreOrderIter(nodes['root']):
            path_dict = self._create_path_dict(fieldnames, node)
            if include_employees:
                employees = self.read_organisation_employees(node.name,
                                                             split_name=False)
                for uuid, employee in employees.items():
                    row = {}
                    address = self.read_user_address(uuid, username=True)
                    org_address = self.read_ou_address(node.name)
                    row.update(path_dict)    # Path
                    row.update(address)      # E-mail, Telefon
                    row.update(org_address)  # Work address
                    row.update(employee)     # Everything else
                    rows.append(row)
            else:
                row = {}
                row.update(path_dict)  # Path
                rows.append(row)
        self._write_csv(fieldnames, rows, filename)

    def main(self):
        # org_id = self.read_organisation()
        # top_units = self.read_top_units(org_id)

        nodes = self.read_ou_tree('f414a2f1-5cac-4634-8767-b8d3109d3133')
        # nodes = self.read_ou_tree('82b42d4e-f7c0-4787-aa2d-9312b284e519')

        filename = 'Alle_lederfunktioner_os2mo.csv'
        self.export_bosses(nodes, filename)

        filename = 'AlleBK-stilling-email_os2mo.csv'
        self.export_all_employees(nodes, filename)

        filename = 'Ballerup_org_incl-medarbejdere_os2mo.csv'
        self.export_orgs(nodes, filename)

        nodes = self.read_ou_tree('4bb95b86-8a1e-4335-a721-a555f46333f6')
        filename = 'SD-løn org med Pnr_os2mo.csv'
        self.export_orgs(nodes, filename, include_employees=False)

if __name__ == '__main__':
    mora_data = MoraQuery()
    mora_data.main()
