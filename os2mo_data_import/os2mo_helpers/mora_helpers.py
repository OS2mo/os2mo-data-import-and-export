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
import csv
import codecs
import logging
import requests
import datetime
from anytree import Node

SAML_TOKEN = os.environ.get('SAML_TOKEN', None)
PRIMARY_RESPONSIBILITY = 'Personale: ansættelse/afskedigelse'

logger = logging.getLogger("mora-helper")


class MoraHelper(object):
    def __init__(self, hostname='http://localhost', export_ansi=True,
                 use_cache=True):
        self.host = hostname + '/service/'
        self.cache = {}
        self.default_cache = use_cache
        self.export_ansi = export_ansi

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
            ou = self.read_ou(sub_node.name)
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

    def _write_csv(self, fieldnames, rows, filename, **options):
        """ Write a csv-file from a a dataset. Only fields explicitly mentioned
        in fieldnames will be saved, the rest will be ignored.
        :param fieldnames: The headline for the columns, also act as filter.
        :param rows: A list of dicts, each list element will become a row, keys
        in dict will be matched to fieldnames.
        :param filename: The name of the exported file.
        """

        if len(options.items()) > 0:
            my_options = options
        else:
            my_options = {"extrasaction": "ignore",
                          "delimiter": ";",
                          "quoting": csv.QUOTE_ALL
                          }

        print('Encode ascii: {}'.format(self.export_ansi))
        with codecs.open(filename, 'w', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, **my_options)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        if self.export_ansi:
            with codecs.open(filename, 'r', encoding='utf-8') as csvfile:
                lines = csvfile.read()
            with codecs.open(filename, 'w',
                             encoding='cp1252') as csvfile:
                csvfile.write(lines)

    def _create_path_dict(self, fieldnames, node, org_types=None):
        """ Create a dict with a MO-path to a given node.
        :param fieldnames: The headline for each level of the path.
        :node: The node to find the path for.
        :return: A dict with headlines as keys and nodes as values.
        """
        ou = self.read_ou(node.name)
        ou_type = ou['org_unit_type']['name']

        if org_types and (ou_type not in org_types):
            return None

        path = self._read_node_path(node)
        path_dict = {}
        i = 0
        for element in path:
            path_dict[fieldnames[i]] = element
            i += 1
        return path_dict

    def _mo_lookup(self, uuid, url, at=None, validity=None, only_primary=False,
                   use_cache=None, calculate_primary=False):
        # TODO: at-value is currently not part of cache key
        if use_cache is None:
            use_cache = self.default_cache

        params = {}
        if calculate_primary:
            params['calculate_primary'] = 1
        if only_primary:
            params['only_primary_uuid'] = 1
        if at:
            params['at'] = at
        elif validity:
            params['validity'] = validity

        full_url = self.host + url.format(uuid)
        if (full_url in self.cache) and use_cache:
            logger.debug("cache hit: %s", full_url)
            return_dict = self.cache[full_url]
        else:
            if SAML_TOKEN is None:
                response = requests.get(full_url, params=params)
                if response.status_code == 401:
                    msg = 'Missing SAML token'
                    logger.error(msg)
                    raise requests.exceptions.RequestException(msg)
                return_dict = response.json()
            else:
                header = {"SESSION": SAML_TOKEN}
                response = requests.get(
                    full_url,
                    headers=header,
                    params=params
                )
                if response.status_code == 401:
                    msg = 'SAML token not accepted'
                    logger.error(msg)
                    raise requests.exceptions.RequestException(msg)
                return_dict = response.json()
            self.cache[full_url] = return_dict
        return return_dict

    def _mo_post(self, url, payload, force=True):
        if force:
            params = {'force': 1}
        else:
            params = None

        if SAML_TOKEN:
            header = {"SESSION": SAML_TOKEN}
        else:
            header = None

        full_url = self.host + url
        response = requests.post(
            full_url,
            headers=header,
            params=params,
            json=payload
        )
        return response

    def read_organisation(self):
        """ Read the main Organisation, all OU's will have this as root.
        Currently reads only one, theroretically more than root org can exist.
        :return: UUID of root organisation
        """
        org_id = self._mo_lookup(uuid=None, url='o')
        return org_id[0]['uuid']

    def read_all_users(self, limit=None):
        """
        Return a list of all employees in MO.
        :param limit: If set, only less large sub-set wll be retrived,
        mainly usefull for testing.
        :return: List af all employees.
        """
        logger.info('Read all MO users')
        org = self.read_organisation()
        if limit is None:
            limit = 100000000
        employee_list = self._mo_lookup(org, 'o/{}/e?limit=' + str(limit))
        employees = employee_list['items']
        logger.info('Done reading all MO users')
        return employees

    def read_it_systems(self):
        """ Read the main Organisation, all OU's will have this as root.
        Currently reads only one, theroretically more than root org can exist.
        :return: UUID of root organisation
        """
        org_id = self.read_organisation()
        it_systems = self._mo_lookup(org_id, url='o/{}/it/')
        return it_systems

    def read_ou(self, uuid, at=None, use_cache=None):
        """ Return a dict with the data available about an OU
        :param uuid: The UUID of the OU
        :return: Dict with the information about the OU
        """
        org_enhed = self._mo_lookup(uuid, 'ou/{}', at, use_cache)
        return org_enhed

    def read_ou_address(self, uuid, at=None, use_cache=None, scope="DAR",
                        return_all=False):
        """ Return a dict with the data available about an OU
        :param uuid: The UUID of the OU
        :param return_all: If True the response will be a list of dicts
        rather than a dict, and all adresses will be returned.
        :return: Dict (or list) with the information about the OU
        """
        return_list = []
        addresses = self._mo_lookup(uuid, 'ou/{}/details/address', at, use_cache)

        for address in addresses:
            return_address = {}
            if address['address_type']['scope'] == scope or scope is None:
                return_address['type'] = address['address_type']['uuid']
                return_address['visibibility'] = address.get('visibility')
                return_address['Adresse'] = address['name']
                return_address['value'] = address['value']
                return_address['uuid'] = address['uuid']
                return_list.append(return_address)

        if return_all:
            return_value = return_list
        else:
            if return_list:
                return_value = return_list[0]
            else:
                return_value = {}
        return return_value

    def read_classes_in_facet(self, facet, use_cache=False):
        """ Return all classes belong to a given facet.
        :param facet: The facet to be returned.
        :return: List of classes in the facet and the uuid of the facet.
        """
        org_uuid = self.read_organisation()
        url = 'o/' + org_uuid + '/f/{}/'
        class_list = self._mo_lookup(facet, url, use_cache=False)
        classes = class_list['data']['items']
        facet_uuid = class_list['uuid']
        return (classes, facet_uuid)

    def read_user(self, user_uuid=None, user_cpr=None, at=None, use_cache=None,
                  org_uuid=None):
        """
        Read basic info for a user. Either uuid or cpr must be given.
        :param user_uuid: UUID of the wanted user.
        :param user_cpr: cpr of the wanted user.
        :return: Basic user info
        """
        if user_uuid:
            user_info = self._mo_lookup(user_uuid, 'e/{}', at, use_cache)
        if user_cpr:
            if not org_uuid:
                org_uuid = self.read_organisation()
            user = self._mo_lookup(user_cpr, 'o/' + org_uuid + '/e?query={}',
                                   at, use_cache)
            assert user['total'] < 2  # Only a single person can be found from cpr

            if user['total'] == 1:
                user_info = self._mo_lookup(user['items'][0]['uuid'], 'e/{}',
                                            at, use_cache)
            else:
                user_info = None
        return user_info

    def terminate_detail(self, mo_type, uuid, from_date):
        """ Terminate a specific MO detail.
        :param mo_type: The MO type to terminate (association, role, engagement, etc)
        :param uuid: Object to terminate.
        :param from_date: Date to terminate from.
        """
        payload = {
            'type': mo_type,
            'uuid': uuid,
            'validity': {
                'to': '2018-01-01'
            }
        }
        logger.info("Terminate detail %s", payload)
        # self._mo_post('details/terminate', payload):

    def read_user_engagement(self, user, at=None, read_all=False, skip_past=False,
                             only_primary=False, use_cache=None,
                             calculate_primary=False):
        """
        Read engagements for a user.
        :param user: UUID of the wanted user.
        :read_all: Read all engagements, not only the present ones.
        :skip_past: Even if read_all is true, do not read the past.
        :calculate_primary: If True, ask MO to calculate primary engagement status.
        :return: List of the users engagements.
        """
        if not read_all:
            engagements = self._mo_lookup(user, 'e/{}/details/engagement',
                                          at, only_primary=only_primary,
                                          use_cache=use_cache,
                                          calculate_primary=calculate_primary)
        else:
            if skip_past:
                validity_times = ['present', 'future']
            else:
                validity_times = ['past', 'present', 'future']

            engagements = []
            for validity in validity_times:
                engagement = self._mo_lookup(user, 'e/{}/details/engagement',
                                             validity=validity,
                                             only_primary=only_primary,
                                             use_cache=False,
                                             calculate_primary=calculate_primary)
                engagements = engagements + engagement
        return engagements

    def read_user_association(self, user, at=None, read_all=False,
                              only_primary=False, use_cache=None):
        """
        Read associations for a user.
        :param user: UUID of the wanted user.
        :return: List of the users associations.
        """
        if not read_all:
            associations = self._mo_lookup(user, 'e/{}/details/association',
                                           at, only_primary=only_primary,
                                           use_cach=use_cache)
        else:
            associations = []
            for validity in ['past', 'present', 'future']:
                association = self._mo_lookup(user, 'e/{}/details/association',
                                              validity=validity,
                                              only_primary=only_primary,
                                              use_cache=False)
                associations = associations + association
        return associations

    def read_user_address(self, user, username=False, cpr=False,
                          at=None, use_cache=None):
        """ Read phone number and email from user
        :param user: UUID of the wanted user
        :return: Dict witn phone number and email (if the exists in MO
        """
        addresses = self._mo_lookup(user, 'e/{}/details/address', at, use_cache)
        return_address = {}
        for address in addresses:
            if address['address_type']['scope'] == 'PHONE':
                return_address['Telefon'] = address['name']
            if address['address_type']['scope'] == 'EMAIL':
                return_address['E-mail'] = address['name']
        if username or cpr:
            personal_info = self._mo_lookup(user, 'e/{}')
            if username:
                return_address['Brugernavn'] = personal_info['user_key']
            if cpr:
                return_address['CPR-Nummer'] = personal_info['cpr_no']
        return return_address

    def read_user_manager_status(self, user):
        """ Returns True of user has responsibility:
        'Personale: ansættelse/afskedigelse'
        :param user: UUID of the wanted user
        :return: True if person is manager accoring to above mentioned rule
        """
        manager_functions = self._mo_lookup(user, 'e/{}/details/manager')
        for manager_function in manager_functions:
            for responsibility in manager_function['responsibility']:
                if responsibility['name'] == PRIMARY_RESPONSIBILITY:
                    return True
        return False

    def read_user_roller(self, user):
        """ Returns the role of the user
        :param user: UUID of the wanted user
        :return: The roles for the user
        """
        roles = self._mo_lookup(user, 'e/{}/details/role')
        role_types = []
        for role in roles:
            role_types.append(role['role_type']['name'])
        return role_types

    def read_engagement_manager(self, engagement_uuid):
        """
        Read the manager corresponding to a given engagement.
        If the engagement user is not a manager, the manager will be the (possibly
        inhereted) manager of the unit. If the person is a manager, the manager will
        be the manager of the nearest higer level department. If no higher level
        managers exists, the person will be a self-manager (typically
        'Kommunaldirektør').
        """
        user_manager = None

        url = 'http://localhost:8080//organisation/organisationfunktion/{}'
        response = requests.get(url.format(engagement_uuid))
        data = response.json()
        relationer = data[engagement_uuid][0]['registreringer'][0]['relationer']
        user = relationer['tilknyttedebrugere'][0]
        unit = relationer['tilknyttedeenheder'][0]

        unit_manager = self.read_ou_manager(unit['uuid'], inherit=True)
        if unit_manager['uuid'] != user['uuid']:
            # In this case the engagement is not manager for itself
            user_manager = unit_manager
        else:
            while unit_manager['uuid'] == user['uuid']:
                mo_unit = self.read_ou(unit['uuid'])
                if mo_unit['parent'] is None:
                    # Self manager!
                    break
                parent_uuid = mo_unit['parent']['uuid']
                unit_manager = self.read_ou_manager(parent_uuid, inherit=True)
            user_manager = unit_manager

        if user_manager is None:
            raise Exception('Unable to find manager')
        return user_manager

    def read_ou_manager(self, unit_uuid, inherit=False):
        """
        Read the manager of an organisation.
        Currently an exception will be raised if an ou has more than
        one manager.
        :param org_uuid: UUID of the OU to find the manager of
        :return: Returns 0 or 1 manager of the OU.
        """
        manager_list = {}
        if inherit:
            managers = self._mo_lookup(unit_uuid,
                                       'ou/{}/details/manager?inherit_manager=1')
        else:
            managers = self._mo_lookup(unit_uuid, 'ou/{}/details/manager')
        # Iterate over all managers, use uuid as key, if more than one
        # distinct uuid shows up in list, rasie an error
        for manager in managers:
            responsibility = {'name': 'Intet ansvar'}
            for responsibility in manager['responsibility']:
                if responsibility['name'] == PRIMARY_RESPONSIBILITY:
                    break
            # TODO: if primary reponsibility is found, this is now selected,
            # otherwise we simply use the last element in the list

            if manager['person'] is not None:
                uuid = manager['person']['uuid']
                data = {'Navn': manager['person']['name'],
                        # 'Ansvar': manager['responsibility'][0]['name'],
                        'Ansvar': responsibility['name'],
                        'uuid': uuid,
                        'relation_uuid': manager['uuid']
                        }
                manager_list[uuid] = data
            else:
                # TODO: This is a vacant manager position
                pass
        if len(manager_list) == 0:
            manager = {}
        elif len(manager_list) == 1:
            manager = manager_list[uuid]
        elif len(manager_list) > 1:
            # Currently we do not support multiple managers
            logger.warning("multiple managers not supported for %s", unit_uuid)
            manager = manager_list[uuid]
            # TODO: Fix this...
            # raise Exception('Too many managers')
        return manager

    def read_organisation_people(self, org_uuid, person_type='engagement',
                                 split_name=True):
        """ Read all employees in an ou. If the same employee is listed
        more than once, only the latest listing will be included.
        :param org_uuid: UUID of the OU to find emplyees in.
        :return: The list of emplyoees
        """
        person_list = {}
        persons = self._mo_lookup(org_uuid, 'ou/{}/details/' + person_type)
        for person in persons:
            uuid = person['person']['uuid']
            data = {
                'Ansættelse gyldig fra': person['validity']['from'],
                'Ansættelse gyldig til': person['validity']['to'],
                'Person UUID': uuid,
                'Org-enhed': person['org_unit']['name'],
                'Org-enhed UUID': person['org_unit']['uuid'],
                'Engagement UUID': person['uuid'],
                'User Key': person['user_key']
            }
            if 'job_function' in person:
                data['Stillingsbetegnelse'] = person['job_function']['name'],

            if 'association_type' in person:
                data['Post'] = person['association_type']['name']

            # Finally, add name
            if split_name:
                # If a split name i wanted, we prefer to get i directly from MO
                # rather than an algorithmic split.
                if person['person'].get('givenname'):
                    data['Fornavn'] = person['person'].get('givenname')
                    data['Efternavn'] = person['person'].get('surname')
                else:
                    data.update(self._split_name(person['person']['name']))
            else:
                data['Navn'] = person['person']['name']
            person_list[uuid] = data
        return person_list

    def read_top_units(self, organisation, use_cache=False):
        """ Read the ous tha refers directoly to the organisation
        :param organisation: UUID of the organisation
        :return: List of UUIDs of the root OUs in the organisation
        """
        url = 'o/{}/children'
        units = self._mo_lookup(organisation, url, use_cache=use_cache)
        return units

    def read_ou_tree(self, org, nodes={}, parent=None):
        """ Recursively find all sub-ou's beneath current node
        :param org: The organisation to start the tree from
        :param nodes: Dict with all modes in the tree
        :param parent: The parent of the current node, None if this is root
        :return: A dict with all nodes in tree, top node is named 'root'
        """
        url = 'ou/{}/children'
        units = self._mo_lookup(org, url)

        if parent is None:
            nodes['root'] = Node(org)
            parent = nodes['root']
        for unit in units:
            uuid = unit['uuid']
            nodes[uuid] = Node(uuid, parent=parent)
            if unit['child_count'] > 0:
                nodes = self.read_ou_tree(uuid, nodes, nodes[uuid])
        return nodes

    def find_cut_dates(self, uuid, no_past=False):
        """
        Run throgh entire history of a user and return a list of dates with
        changes in the engagement.
        """
        mo_engagement = self.read_user_engagement(
            user=uuid,
            only_primary=True,
            read_all=True,
            skip_past=no_past
        )

        dates = set()
        for eng in mo_engagement:
            dates.add(datetime.datetime.strptime(eng['validity']['from'],
                                                 '%Y-%m-%d'))
            if eng['validity']['to']:
                to = datetime.datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
                day_after = to + datetime.timedelta(days=1)
                dates.add(day_after)
            else:
                dates.add(datetime.datetime(9999, 12, 30, 0, 0))

        date_list = sorted(list(dates))
        logger.debug('List of cut-dates: {}'.format(date_list))
        return date_list

    def get_e_username(self, e_uuid, id_it_system):
        for its in self._mo_lookup(e_uuid, 'e/{}/details/it'):
            if its['itsystem']["user_key"] == id_it_system:
                return its['user_key']
        return ''

    def get_e_address(self, e_uuid, scope):
        for address in self._mo_lookup(e_uuid, 'e/{}/details/address'):
            if address['address_type']['scope'] == scope:
                return address
        return {}
