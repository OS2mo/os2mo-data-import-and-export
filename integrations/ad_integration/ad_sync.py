import json
import pathlib
import logging
from datetime import datetime

import ad_reader
import ad_logger
from os2mo_helpers.mora_helpers import MoraHelper
from exporters.sql_export.lora_cache import LoraCache

from integrations.ad_integration import read_ad_conf_settings

logger = logging.getLogger('AdSyncRead')


# how to check these classes for noobs
# look at :https://os2mo-test.holstebro.dk/service/o/ORGUUID/f/
# It must be addresses, so we find the address thing for employees
# https://os2mo-test.holstebro.dk/service/o/ORGUUID/f/employee_address_type/
# There You have it - for example the mobile phone
# Now You may wonder if the VISIBLE/SECRET are right:
# Find them here https://os2mo-test.holstebro.dk/service/o/ORGUUID/f/visibility/


# AD has no concept of temporality, validity is always from now to infinity.
VALIDITY = {
    'from':  datetime.strftime(datetime.now(), "%Y-%m-%d"),
    'to': None
}


class AdMoSync(object):
    def __init__(self, all_settings=None):
        logger.info('AD Sync Started')

        self.settings = all_settings
        if self.settings is None:
            self.settings = read_ad_conf_settings.read_settings()

        self.mapping = self.settings['integrations.ad.ad_mo_sync_mapping']

        self.helper = self._setup_mora_helper()
        self.org = self.helper.read_organisation()

        self._verify_it_systems()
        self._setup_ad_reader_and_cache_all()

        # Possibly get IT-system directly from LoRa for better performance.
        lora_speedup = self.settings.get(
            'integrations.ad.ad_mo_sync_direct_lora_speedup', False)
        if lora_speedup:
            print('Retrive LoRa dump')
            self.lc = LoraCache(resolve_dar=False, full_history=False)
            self.lc.populate_cache(dry_run=False, skip_associations=True)
            self.lc.calculate_primary_engagements()
            print('Done')
        else:
            print('Use direct MO access')
            self.lc = None

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

        used_mo_fields = []
        for key in self.mapping.keys():
            for ad_field, mo_combi in self.mapping.get(key, {}).items():
                if mo_combi in used_mo_fields:
                    msg = 'MO field {} used more than once'
                    raise Exception(msg.format(mo_combi))
                used_mo_fields.append(mo_combi)

        self.stats = {
            'addresses': [0, 0],
            'engagements': 0,
            'it_systems': 0,
            'users': set()
        }

    def _verify_it_systems(self):
        if 'it_systems' not in self.mapping:
            return

        mo_it_systems = self.helper.read_it_systems()

        for it_system, it_system_uuid in self.mapping['it_systems'].items():
            found = any(map(
                lambda mo_it_system: mo_it_system['uuid'] == it_system_uuid,
                mo_it_systems
            ))
            if not found:
                msg = '{} with uuid {}, not found in MO'
                raise Exception(msg.format(it_system, it_system_uuid))

    def _setup_ad_reader_and_cache_all(self):
        skip_school = self.settings.get('integrations.ad.skip_school_ad_to_mo', True)
        logger.info('Skip school domain: {}'.format(skip_school))
        self.ad_reader = ad_reader.ADParameterReader(skip_school=skip_school)
        print('Retrive AD dump')
        self.ad_reader.cache_all()
        print('Done')
        logger.info('Done with AD caching')

    def _setup_mora_helper(self):
        return MoraHelper(hostname=self.settings['mora.base'],
                          use_cache=False)

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
        if self.lc:
            employees = self.lc.users.values()
        else:
            employee_list = self.helper._mo_lookup(
                self.org, 'o/{}/e?limit=1000000000')
            employees = employee_list['items']
        logger.info('Done reading all MO users')
        return employees

    def _find_existing_ad_address_types(self, uuid):
        """
        Find the addresses that is already related to a user.
        :param uuid: The uuid of the user in question.
        :return: A dictionary with address classes as keys and tuples of address
        objects and values as values.
        """
        types_to_edit = {}
        if self.lc:
            user_addresses = []
            for addr in self.lc.addresses.values():
                if addr[0]['user'] == uuid:
                    user_addresses.append(
                        {
                            'uuid': addr[0]['uuid'],
                            'address_type': {'uuid': addr[0]['adresse_type']},
                            'visibility': {'uuid': addr[0]['visibility']},
                            'value': addr[0]['value']
                        }
                    )
        else:
            user_addresses = self.helper._mo_lookup(uuid, 'e/{}/details/address')

        for field, klasse in self.mapping['user_addresses'].items():
            address_type_uuid, visibility_uuid = klasse
            potential_matches = user_addresses
            # Filter out addresses with wrong type
            def check_address_type_uuid(address):
                return address['address_type']['uuid'] == address_type_uuid
            potential_matches = filter(check_address_type_uuid, potential_matches)
            # Filter out addresses with wrong visibility
            def check_address_visibility(address):
                return (
                    visibility_uuid is None or 'visibility' not in address or
                    self.visibility[visibility_uuid] == address['visibility']['uuid']
                )
            potential_matches = filter(check_address_visibility, potential_matches)
            # Consume iterator, verifying either 0 or 1 elements are returned
            found_address = next(potential_matches, None)
            assert next(potential_matches, None) == None
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

    def _edit_address(self, address_uuid, value, klasse, validity=VALIDITY):
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
                    'validity': validity,
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

    def _edit_engagement(self, uuid, ad_object):
        if self.lc:
            eng = None
            for cur_eng in self.lc.engagements.values():
                if cur_eng[0]['user'] == uuid:
                    if cur_eng[0]['primary_boolean']:
                        eng = cur_eng[0]

            if eng is None:
                # No current primary engagment found
                return False
            # Notice, this will only update current row, if more rows exists, they
            # will not be updated until the first run after that row has become
            # current. To fix this, we will need to ad option to LoRa cache to be
            # able to return entire object validity (poc-code exists).
            validity = {
                'from': VALIDITY['from'],
                'to': eng['to_date']
            }
            for ad_field, mo_field in self.mapping['engagements'].items():
                if mo_field == 'extension_1':
                    mo_value = eng['extensions']['udvidelse_1']
                if mo_field == 'extension_2':
                    mo_value = eng['extensions']['udvidelse_2']
                if mo_field == 'extension_3':
                    mo_value = eng['extensions']['udvidelse_3']
                if mo_field == 'extension_4':
                    mo_value = eng['extensions']['udvidelse_4']
                if mo_field == 'extension_5':
                    mo_value = eng['extensions']['udvidelse_5']
                if mo_field == 'extension_6':
                    mo_value = eng['extensions']['udvidelse_6']
                if mo_field == 'extension_7':
                    mo_value = eng['extensions']['udvidelse_7']
                if mo_field == 'extension_8':
                    mo_value = eng['extensions']['udvidelse_8']
                if mo_field == 'extension_9':
                    mo_value = eng['extensions']['udvidelse_9']
                if mo_field == 'extension_10':
                    mo_value = eng['extensions']['udvidelse_10']

                if not ad_object.get(ad_field):
                    logger.info('{} not in ad_object'.format(ad_field))
                    continue
                payload = {
                    'type': 'engagement',
                    'uuid': eng['uuid'],
                    'data': {
                        mo_field: ad_object.get(ad_field),
                        'validity': validity
                    }
                }
                if mo_value == ad_object.get(ad_field):
                    continue
                logger.debug('Edit payload: {}'.format(payload))
                response = self.helper._mo_post('details/edit', payload)
                self.stats['engagements'] += 1
                self.stats['users'].add(uuid)
                logger.debug('Response: {}'.format(response.text))
        else:
            print('No cache')
            user_engagements = self.helper._mo_lookup(
                uuid, 'e/{}/details/engagement?calculate_primary=1')
            for eng in user_engagements:
                if not eng['is_primary']:
                    continue

                validity = {
                    'from': VALIDITY['from'],
                    'to': eng['validity']['to']
                }
                for ad_field, mo_field in self.mapping['engagements'].items():
                    if ad_object.get(ad_field):
                        payload = {
                            'type': 'engagement',
                            'uuid': eng['uuid'],
                            'data': {
                                mo_field: ad_object.get(ad_field),
                                'validity': validity
                            }
                        }
                        if not eng[mo_field] == ad_object.get(ad_field):
                            logger.debug('Edit payload: {}'.format(payload))
                            response = self.helper._mo_post('details/edit', payload)
                            self.stats['engagements'] += 1
                            self.stats['users'].add(uuid)
                            logger.debug('Response: {}'.format(response.text))
                        else:
                            print('Ingen ændring')

                    else:
                        logger.info('{} not in ad_object'.format(ad_field))

    def _edit_it_system(self, uuid, ad_object):
        mo_itsystem_uuid = self.mapping['it_systems']['samAccountName']
        if self.lc:
            username = ''
            for it in self.lc.it_connections.values():
                if it[0]['user'] == uuid:
                    if it[0]['itsystem'] == mo_itsystem_uuid:
                        username = it[0]['username']
        else:
            username = self.helper.get_e_username(uuid, 'Active Directory')
        # If username is blank, we have found a user that needs to be assiged to an
        # IT-system.
        if username == '':
            payload = {
                'type': 'it',
                'user_key': ad_object['SamAccountName'],
                'itsystem': {'uuid': mo_itsystem_uuid},
                'person': {'uuid': uuid},
                'validity': VALIDITY
            }
            logger.debug('Create it system payload: {}'.format(payload))
            response = self.helper._mo_post('details/create', payload)
            self.stats['it_systems'] += 1
            self.stats['users'].add(uuid)
            logger.debug('Response: {}'.format(response.text))
            response.raise_for_status()

    def _edit_user_addresses(self, uuid, ad_object):
        fields_to_edit = self._find_existing_ad_address_types(uuid)

        for field, klasse in self.mapping['user_addresses'].items():
            if not ad_object.get(field):
                logger.debug('No such AD field: {}'.format(field))
                continue

            if field not in fields_to_edit.keys():
                # This is a new address
                self.stats['addresses'][0] += 1
                self.stats['users'].add(uuid)
                self._create_address(uuid, ad_object[field], klasse)
            else:
                # This is an existing address
                if not fields_to_edit[field]['value'] == ad_object[field]:
                    msg = 'Value change, MO: {} <> AD: {}'
                    self.stats['addresses'][1] += 1
                    self.stats['users'].add(uuid)
                    self._edit_address(fields_to_edit[field]['uuid'],
                                       ad_object[field],
                                       klasse)
                else:
                    msg = 'No value change: {}=={}'
                logger.debug(msg.format(fields_to_edit[field]['value'],
                                        ad_object[field]))

    def _finalize_it_system(self, uuid, ad_object):
        if 'it_systems' not in self.mapping:
            return

        # TODO: Fetch current and check validity

        mo_itsystem_uuid = self.mapping['it_systems']['samAccountName']
        today = datetime.strftime(datetime.now(), "%Y-%m-%d")
        payload = {
            'type': 'it',
            'uuid': mo_itsystem_uuid,
            'validity': {"to": today}
        }
        logger.debug('Finalize payload: {}'.format(payload))
        response = self.helper._mo_post('details/terminate', payload)
        logger.debug('Response: {}'.format(response.text))

    def _finalize_user_addresses(self, uuid, ad_object):
        if 'user_addresses' not in self.mapping:
            return

        fields_to_edit = self._find_existing_ad_address_types(uuid)
        for field, klasse in self.mapping['user_addresses'].items():
            if not ad_object.get(field):
                logger.debug('No such AD field: {}'.format(field))
                continue
            if field not in fields_to_edit.keys():
                continue
            # If an end date is already set, do not change.
            # NOTE: Maybe this should be not set, or in the future?
            if fields_to_edit[field]['validity']['to'] is not None:
                continue
            today = datetime.strftime(datetime.now(), "%Y-%m-%d")
            payload = {
                'type': 'address',
                'uuid': fields_to_edit[field]['uuid'],
                'validity': {"to": today}
            }
            logger.debug('Finalize payload: {}'.format(payload))
            response = self.helper._mo_post('details/terminate', payload)
            logger.debug('Response: {}'.format(response.text))

    def _update_single_user(self, uuid, ad_object):
        """
        Update all fields for a single user.
        :param uuid: uuid of the user.
        :param ad_object: Dict with the AD information for the user.
        """
        # Debug log if enabled is not found
        if 'Enabled' not in ad_object:
            logger.info('{} not in ad_object'.format("Enabled"))

        # Lookup whether or not to syncronize disabled users
        sync_disabled = self.settings.get("integrations.ad.ad_mo_sync_disabled", True)
        # Check whether the current user is disabled or not
        current_user_is_disabled = ad_object.get('Enabled', True) == False
        if sync_disabled == False and current_user_is_disabled:
            finalize_key = 'integrations.ad.ad_mo_sync_finalize_disabled' 
            if finalize_key not in self.settings:
                raise Exception("{} not in settings".format(finalize_key))
            # If configured to finalize
            if self.settings[finalize_key]:
                # Set validity end --> today if in the future
                self._finalize_it_system(uuid, ad_object)
                self._finalize_user_addresses(uuid, ad_object)
            # Otherwise do not do anything
            return

        # Sync the user, whether disabled or not
        if 'it_systems' in self.mapping:
            self._edit_it_system(uuid, ad_object)

        if 'engagements' in self.mapping:
            self._edit_engagement(uuid, ad_object)

        if 'user_addresses' in self.mapping:
            self._edit_user_addresses(uuid, ad_object)

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
            # logger.info('Start sync of {}'.format(employee['uuid']))
            if 'cpr' in employee:
                cpr = employee['cpr']
            else:
                user = self.helper.read_user(employee['uuid'])
                cpr = user['cpr_no']
            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            if response:
                self._update_single_user(employee['uuid'], response)
            # logger.info('End sync of {}'.format(employee['uuid']))
        logger.info('Stats: {}'.format(self.stats))
        self.stats['users'] = 'Written in log file'
        print(self.stats)


if __name__ == '__main__':
    ad_logger.start_logging('ad_mo_sync.log')

    sync = AdMoSync()
    sync.update_all_users()
