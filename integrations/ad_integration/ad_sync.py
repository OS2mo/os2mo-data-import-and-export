import json
import pathlib
import logging
import requests
from datetime import datetime

import ad_reader
import ad_logger
from os2mo_helpers.mora_helpers import MoraHelper


logger = logging.getLogger('AdSyncRead')


# how to check these classes for noobs
# look at :https://os2mo-test.holstebro.dk/service/o/ORGUUID/f/
# It must be addresses, so we find the address thing for employees
# https://os2mo-test.holstebro.dk/service/o/ORGUUID/f/employee_address_type/
# There You have it - for example the mobile phone
# Now You may wonder if the VISIBLE/SECRET are right:
# Find them here https://os2mo-test.holstebro.dk/service/o/ORGUUID/f/visibility/


# AD has  no concept of temporality, validity is always from now to infinity.
VALIDITY = {
    'from':  datetime.strftime(datetime.now(), "%Y-%m-%d"),
    'to': None
}


class AdMoSync(object):
    def __init__(self):
        logger.info('AD Sync Started')
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())
        self.mapping = self.settings['integrations.ad.ad_mo_sync_mapping']

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)
        self.org = self.helper.read_organisation()

        # Possibly get IT-system directly from LoRa for better performance.
        lora_speedup = self.settings.get(
            'integrations.ad.ad_mo_sync_direct_lora_speedup', False)
        if lora_speedup:
            self.mo_ad_users = self._cahce_it_systems()
        else:
            self.mo_ad_users = {}

        mo_visibilities = self.helper.read_classes_in_facet('visibility')[0]
        self.visibility = {
            'PUBLIC': self.settings['address.visibility.public'],
            'INTERNAL': self.settings['address.visibility.internal'],
            'SECRET': self.settings['address.visibility.secret']
        }
        for sync_vis in self.visibility.values():
            found = False
            for mo_vis in mo_visibilities:
                if sync_vis == mo_vis['uuid']:
                    found = True
            if not found:
                raise Exception('Error in visibility class configuration')

        skip_school = self.settings.get('integrations.ad.skip_school_ad_to_mo', True)
        logger.info('Skip school domain: {}'.format(skip_school))
        self.ad_reader = ad_reader.ADParameterReader(skip_school=skip_school)

        self.ad_reader.cache_all()
        logger.info('Done with AD caching')

    def _cahce_it_systems(self):
        logger.info('Cache all it-systems')
        mo_ad_users = {}
        # Get LoRa url from settings
        url = '/organisation/organisationfunktion?funktionsnavn=IT-system'
        response = requests.get(self.settings['mox.base'] + url)
        uuid_list = response.json()
        it_systems = []
        build_up = '?'

        for uuid in uuid_list['results'][0]:
            if build_up.count('&') < 96:
                build_up += 'uuid=' + uuid + '&'
                continue

            url = '/organisation/organisationfunktion' + build_up[:-1]
            response = requests.get(self.settings['mox.base'] + url)
            data = response.json()
            it_systems += data['results'][0]
            build_up = '?'

        for system in it_systems:
            reg = system['registreringer'][0]
            user_key = (reg['attributter']['organisationfunktionegenskaber'][0]
                        ['brugervendtnoegle'])
            user_uuid = reg['relationer']['tilknyttedebrugere'][0]['uuid']
            it_system = reg['relationer']['tilknyttedeitsystemer'][0]['uuid']

            if it_system == self.mapping['it_systems']['samAccountName']:
                mo_ad_users[user_uuid] = user_key

        logger.info('Done Cacheing all it-systems')
        return mo_ad_users

    def _read_mo_classes(self):
        """
        Read all address classes in MO. Mostly usefull for debugging.
        """
        # This is not really needed, unless we want to make a consistency check.
        emp_adr_classes = self.helper.read_classes_in_facet('employee_address_type')
        for emp_adr_class in emp_adr_classes[0]:
            print(emp_adr_class)

    def _read_all_mo_users(self):
        """
        Return a list of all employees in MO.
        :return: List af all employees.
        """
        logger.info('Read all MO users')
        employee_list = self.helper._mo_lookup(self.org, 'o/{}/e?limit=1000000000')
        employees = employee_list['items']
        logger.info('Done reading all MO users')
        return employees

    def _find_existing_ad_address_types(self, uuid):
        """
        Find the addresses that is already related to a user.
        :param uuid: The uuid of the user in question.
        :return: A dictionary with address classes as keys and tuples of adress
        objects and values as values.
        """
        # Unfortunately, mora-helper currently does not read all addresses
        types_to_edit = {}
        user_addresses = self.helper._mo_lookup(uuid, 'e/{}/details/address')
        for field, klasse in self.mapping['user_addresses'].items():
            found_address = None
            for address in user_addresses:
                if not address['address_type']['uuid'] == klasse[0]:
                    continue
                if klasse[1] is not None and 'visibility' in address:
                    if self.visibility[klasse[1]] == address['visibility']['uuid']:
                        found_address = (address['uuid'], address['value'])
                else:
                    found_address = (address['uuid'], address['value'])
            if found_address is not None:
                types_to_edit[field] = found_address
        logger.debug('Existing fields for {}: {}'.format(uuid, types_to_edit))
        return types_to_edit

    def _create_address(self, uuid, value, klasse):
        """
        Create a new address for a user.
        :param uuid: uuid of the user.
        :param: value Value of of the adress.
        :param: klasse: The address type and vissibility of the address.
        """
        payload = {
            'value': value,
            'address_type': {'uuid': klasse[0]},
            'person': {'uuid': uuid},
            'type': 'address',
            'validity': VALIDITY,
            'org': {'uuid': self.org}
        }
        if klasse[1] is not None:
            payload['visibility'] = {'uuid': self.visibility[klasse[1]]}
        logger.debug('Create payload: {}'.format(payload))
        response = self.helper._mo_post('details/create', payload)
        logger.debug('Response: {}'.format(response))

    def _edit_address(self, address_uuid, value, klasse):
        """
        Edit an exising address to a new value.
        :param address_uuid: uuid of the address object.
        :param value: The new value
        :param: klasse: The address type and vissibility of the address.
        """
        payload = [
            {
                'type': 'address',
                'uuid': address_uuid,
                'data': {
                    'validity': VALIDITY,
                    'value': value,
                    'address_type': {'uuid': klasse[0]}
                }
            }
        ]
        if klasse[1] is not None:
            payload[0]['data']['visibility'] = {'uuid': self.visibility[klasse[1]]}
        logger.debug('Edit payload: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        logger.debug('Response: {}'.format(response.text))

    def _update_single_user(self, uuid, ad_object):
        """
        Update all fields for a single user.
        :param uuid: uuid of the user.
        :param ad_object: Dict with the AD information for the user.
        """

        # First, check for AD account:
        if self.mo_ad_users:
            username = self.mo_ad_users.get(uuid, '')
        else:
            username = self.helper.get_e_username(uuid, 'Active Directory')
        # If username is blank, we have found a user that needs to be assiged to an
        # IT-system.
        if username is '':
            payload = {
                'type': 'it',
                'user_key': ad_object['SamAccountName'],
                'itsystem': {'uuid': self.mapping['it_systems']['samAccountName']},
                'person': {'uuid': uuid},
                'validity': VALIDITY
            }
            logger.debug('Create it system payload: {}'.format(payload))
            response = self.helper._mo_post('details/create', payload)
            logger.debug('Response: {}'.format(response.text))
            response.raise_for_status()

        fields_to_edit = self._find_existing_ad_address_types(uuid)

        # Assume no existing adresses, we fix that later
        for field, klasse in self.mapping['user_addresses'].items():
            if field not in ad_object:
                logger.debug('No such AD field: {}'.format(field))
                continue

            if field not in fields_to_edit.keys():
                # This is a new address
                self._create_address(uuid, ad_object[field], klasse)
            else:
                # This is an existing address
                if not fields_to_edit[field][1] == ad_object[field]:
                    self._edit_address(fields_to_edit[field][0],
                                       ad_object[field],
                                       klasse)
                else:
                    msg = 'No value change: {}=={}'
                    logger.debug(msg.format(fields_to_edit[field][1],
                                            ad_object[field]))

    def update_all_users(self):
        """
        Iterate over all users and sync AD informations to MO.
        """
        i = 0
        employees = self._read_all_mo_users()
        for employee in employees:
            i = i + 1
            if i % 100 == 0:
                print('Progress: {}/{}'.format(i, len(employees)))
            logger.info('Start sync of {}'.format(employee['uuid']))
            user = self.helper.read_user(employee['uuid'])
            response = self.ad_reader.read_user(cpr=user['cpr_no'], cache_only=True)
            if response:
                self._update_single_user(employee['uuid'], response)
            logger.info('End sync of {}'.format(employee['uuid']))


if __name__ == '__main__':
    ad_logger.start_logging('ad_mo_sync.log')

    sync = AdMoSync()
    sync.update_all_users()
