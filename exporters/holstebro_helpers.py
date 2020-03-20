# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
Holstebro specific export functions, decorators and 
helper classes
"""
import json
import logging
import os
import queue
import threading
import pathlib
import time

import requests
from anytree import Node, PreOrderIter
from datetime import datetime, timedelta

from os2mo_helpers.mora_helpers import MoraHelper

SAML_TOKEN = os.environ.get('SAML_TOKEN', None)


cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
SETTINGS = json.loads(cfg_file.read_text())

leader_logger = logging.getLogger('LederHierarki')
logger = logging.getLogger('Exports')


def export_to_planorama(mh, nodes, filename_org, filename_persons):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    : param nodes: The nodes of the OU tree
    """
    fieldnames_persons = ['UUID',
                          'Username',
                          # 'Password',
                          'Name',
                          'Title',
                          'Address',
                          # 'Zip',
                          # 'Country',
                          # 'CPR',
                          'Email',
                          # 'Number',
                          'Mobile',
                          'Telephone',
                          'Responsible',
                          'Responsible_UUID',
                          'Company']
    fieldnames_org = ['Root', 'Number', 'Name', 'Manager_uuid']

    rows_org = []
    rows_persons = []

    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        # Do not add "Afdelings-niveau"
        if ou['org_unit_level']['name'] != 'Afdelings-niveau':
            over_uuid = ou['parent']['uuid'] if ou['parent'] else ''

            employees = mh.read_organisation_people(node.name, 'engagement', False)
            # Does this node have a new name?
            manager = find_org_manager(mh, node)
            if(manager['uuid'] != ''):
                manager_engagement = mh.read_user_engagement(manager['uuid'])
            else:
                manager_engagement = [{'user_key': ''}]

            row_org = {'Root': over_uuid,
                       'Number': ou['uuid'],
                       'Name': ou['name'],
                       'Manager_uuid': manager['uuid'] if 'uuid' in manager else ''}
            rows_org.append(row_org)

            for uuid, employee in employees.items():
                row = {}
                address = mh.read_user_address(uuid, username=True, cpr=True)
                # manager = mh.read_engagement_manager(employee['Engagement UUID'])

                row = {'UUID': uuid,
                       'Username': employee['User Key'],
                       # 'Password': '',
                       'Name': employee['Navn'],
                       'Title': employee['Stillingsbetegnelse'][0] if len(employee['Stillingsbetegnelse']) > 0 else '',
                       'Address': address['Lokation'] if 'Lokation' in address else '',
                       # 'Zip': '',
                       # 'Country': '',
                       # 'CPR': address['CPR-Nummer'],
                       'Email': address['E-mail'] if 'E-mail' in address else '',
                       # 'Number': '',
                       'Mobile': address['Mobiltelefon'] if 'Mobiltelefon' in address else '',
                       'Telephone': address['Telefon'] if 'Telefon' in address else '',
                       'Responsible': manager_engagement[0]['user_key'] if len(manager_engagement) > 0 else '',
                       'Responsible_UUID': manager['uuid'] if 'uuid' in manager else '',
                       'Company': employee['Org-enhed UUID']
                       }

                rows_persons.append(row)

    mh._write_csv(fieldnames_persons, rows_persons, filename_persons)
    mh._write_csv(fieldnames_org, rows_org, filename_org)


def update_org_with_hk_managers(mh, nodes):
    """ Traverses a tree of OUs, for each OU finds the manager of the OU.
    : param nodes: The nodes of the OU tree
    """
    managerHelper = HolstebroManagerHelper(mh)

    for node in PreOrderIter(nodes['root']):
        ou = mh.read_ou(node.name)
        # for each ou, check if name contains _leder
        # if so, check ou for "tilknytninger" and set  this as leader for ou.parent
        if ou['org_unit_level']['name'] == 'Afdelings-niveau' and ou['name'].count('_leder') == 1:
            # We have an Afdeling with name _leder, find associated employees and make them leaders in the parent ou
            associated_employees = mh.read_organisation_people(
                node.name, 'association', False)

            if(ou['parent'] != None and len(associated_employees) == 1):
                manager_uuid = list(associated_employees)[0]
                managerHelper.update_manager(
                    ou['parent'], associated_employees[manager_uuid])

            # This manager is now manager for parent ou
            # if parent ou's name ends with "led/adm", make employee
            # manager for the parent ou's parent as well.
            if ou['parent']['parent'] != None and ou['parent']['name'].count('led-adm') == 1:
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

    if(node.is_root):
        print("root node")
        # return {'uuid': ''}

    new_manager = mh.read_ou_manager(node.name, True)

    if new_manager:
        return new_manager
    elif node.depth != 0:
        return find_org_manager(mh, node.parent)
    else:
        return {'uuid': ''}


class HolstebroManagerHelper(object):
    def __init__(self, mh):
        self.mh = mh
        self.manager_info = self._get_manager_types()

    def _get_manager_types(self):
        org_uuid = self.mh.read_organisation()
        manager_types = self.mh._mo_lookup(org_uuid, 'o/{}/f/manager_type')
        manager_levels = self.mh._mo_lookup(org_uuid, 'o/{}/f/manager_level')

        return_dict = {'manager_types': {}, 'manager_levels': {}}

        for mt in manager_types['data']['items']:
            return_dict['manager_types'].update({mt['name']: mt})

        for ml in manager_levels['data']['items']:
            return_dict['manager_levels'].update({ml['name']: ml})

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
            "responsibility": [],
            "user_key": "HK-AUTO-ASSIGNED",
            "validity": {
                "from": self._get_date()
            }
        }
        leader_logger.debug(
            "Attempting to create manager for ou uuid: {}".format(ou_uuid))
        self.mh._mo_post('details/create', payload, False)
        leader_logger.debug("Manager created")

    def _terminate_manager(self, manager_releation_uuid):
        payload = {
            'type': 'manager',
            'uuid': manager_releation_uuid,
            'validity': {
                    'to': self._get_date(1)
            }
        }

        leader_logger.debug(
            "Attempting to terminate manager relation uuid: {}".format(manager_releation_uuid))
        self.mh._mo_post('details/terminate', payload, True)
        leader_logger.debug("Manager terminated")

    def _get_date(self, td=0):
        rdate = datetime.today() - timedelta(days=td)
        return rdate.strftime('%Y-%m-%d')

    def update_manager(self, ou, manager):
        """Checks if manager is manger for the given ou
        Update the ou with new manager and deletes ones found
        """

        ou_uuid = ou['uuid']
        manager_uuid = manager['Person UUID']

        # Create list of managers from this Afdelings-niveau
        # update parent ou
        # Get non-inherited manager, ALWAYS returns 1 or no manager
        ou_manager = self.mh.read_ou_manager(ou_uuid, False)

        if ou_manager == {}:  # no manager, create it
            leader_logger.debug("Manager for {} should be {}".format(
                ou['name'], manager['Navn']))

            manager_level = self._get_org_level(ou)
            self._create_manager(ou_uuid, manager_uuid, manager_level)

        elif ou_manager['uuid'] != manager_uuid:
            leader_logger.debug("Manager for {} should be {}".format(
                ou['name'], manager['Navn']))

            self._terminate_manager(ou_manager['relation_uuid'])

            manager_level = self._get_org_level(ou)
            self._create_manager(ou_uuid, manager_uuid, manager_level)
