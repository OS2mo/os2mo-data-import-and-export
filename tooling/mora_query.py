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

    def _write_csv(self, fieldnames, rows, filename):
        with open(filename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

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

    def read_user_address(self, user):
        """ Read phone number and email from user
        :param user: UUID of the wanted user
        :return: Dict witn phone number and email (if the exists in MO
        """
        url = self.host + 'e/' + user + '/details//address'
        addresses = requests.get(url).json()
        return_address = {}
        for address in addresses:
            if address['address_type']['scope'] == 'PHONE':
                return_address['Telefon'] = address['name']
            if address['address_type']['scope'] == 'EMAIL':
                return_address['E-mail'] = address['name']
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
            raise Exception('Too many managers')
        return manager

    def read_organisation_employees(self, org_uuid):
        """ Read all employees in an ou. If the same employee is listed
        more than once, only the latest listing will be included.
        :param org_uuid: UUID of the OU to find emplyees in.
        :return: The list of emplyoees
        """
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
            # Finally, add first and last name
            data.update(self._split_name(employee['person']['name']))
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

    def export_all_employees(self, nodes):
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
        # BRUGERNAVN!!!!
        for node in PreOrderIter(nodes['root']):
            employees = self.read_organisation_employees(node.name)
            for uuid, employee in employees.items():
                row = {}
                address = self.read_user_address(uuid)
                row.update(address)  # E-mail, Telefon
                row.update(employee)  # Everything else
                rows.append(row)
        self._write_csv(fieldnames, rows, 'medarbejdere.csv')

    def export_bosses(self, nodes):
        """ Traverses a tree of OUs, for each OU finds the manager of the OU.
        The list of managers will be saved o a csv-file.
        :param nodes: The nodes of the OU tree
        """
        fieldnames = ['root', 'org', 'sub org']
        for i in range(2, nodes['root'].height - 1):
            fieldnames += [str(i) + 'xsub org']
        fieldnames += ['Ansvar', 'Navn', 'Telefon', 'E-mail']

        rows = []
        for node in PreOrderIter(nodes['root']):
            manager = self.read_organisation_managers(node.name)
            if manager:
                row = {}
                path = self._read_node_path(node)
                i = 0
                for element in path:
                    row[fieldnames[i]] = element
                    i += 1
                address = self.read_user_address(manager['uuid'])
                row.update(manager)  # Navn, Ansvar
                row.update(address)  # E-mail, Telefon
                del row['uuid']
                rows.append(row)
        self._write_csv(fieldnames, rows, 'alle_lederfunktioner.csv')

    def main(self):
        # org_id = self.read_organisation()
        # top_units = self.read_top_units(org_id)

        nodes = self.read_ou_tree('f414a2f1-5cac-4634-8767-b8d3109d3133')
        #self.export_bosses(nodes)
        self.export_all_employees(nodes)


if __name__ == '__main__':
    mora_data = MoraQuery()
    mora_data.main()
