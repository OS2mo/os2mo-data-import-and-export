import json
import pathlib
import logging
from datetime import datetime
from operator import itemgetter
from functools import partial

from more_itertools import only, partition
from tqdm import tqdm

import ad_reader as adreader
import ad_logger
from os2mo_helpers.mora_helpers import MoraHelper
from exporters.sql_export.lora_cache import LoraCache
from exporters.utils.jinja_filter import create_filters
from exporters.utils.apply import apply

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
            self.settings = read_ad_conf_settings.SETTINGS

        self.helper = self._setup_mora_helper()
        self.org = self.helper.read_organisation()

        seeded_create_filters = partial(create_filters, tuple_keys=("uuid", "ad_object"))
        self.pre_filters = seeded_create_filters(
            self.settings.get("integrations.ad.ad_mo_sync.pre_filters", [])
        )
        self.terminate_disabled_filters = seeded_create_filters(
            self.settings.get("integrations.ad.ad_mo_sync.terminate_disabled_filters", [])
        )

        # Possibly get IT-system directly from LoRa for better performance.
        self.lc = self._setup_lora_cache()

        mo_visibilities = self.helper.read_classes_in_facet('visibility')[0]
        self.visibility = {
            'PUBLIC': self.settings['address.visibility.public'],
            'INTERNAL': self.settings['address.visibility.internal'],
            'SECRET': self.settings['address.visibility.secret']
        }

        # Check that the configured visibilities are found in MO
        configured_visibilities = set(self.visibility.values())
        mo_visibilities = set(map(itemgetter('uuid'), mo_visibilities))
        # If the configured visibiltities are not a subset, atleast one is missing.
        if not configured_visibilities.issubset(mo_visibilities):
            raise Exception('Error in visibility class configuration')

    def _setup_mora_helper(self):
        return MoraHelper(hostname=self.settings['mora.base'],
                          use_cache=False)

    def _setup_lora_cache(self):
        # Possibly get IT-system directly from LoRa for better performance.
        lora_speedup = self.settings.get(
            'integrations.ad.ad_mo_sync_direct_lora_speedup', False
        )
        if lora_speedup:
            print('Retrive LoRa dump')
            lc = LoraCache(resolve_dar=False, full_history=False)
            lc.populate_cache(dry_run=False, skip_associations=True)
            # skip reading lora - not for prod
            # lc.populate_cache(dry_run=True, skip_associations=True)
            lc.calculate_primary_engagements()
            print('Done')
            return lc
        print('Use direct MO access')
        return None

    def _read_all_mo_users(self):
        """Return a list of all employees in MO.

        :return: List af all employees.
        """
        logger.info('Read all MO users')
        if self.lc:
            employees = list(map(itemgetter(0), self.lc.users.values()))
        else:
            employees = self.helper.read_all_users()
        logger.info('Done reading all MO users')
        return employees

    def _find_existing_ad_address_types(self, uuid):
        """Find the addresses that is already related to a user.

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
                            'value': addr[0]['value'],
                            'validity': {
                                "from": addr[0]['from_date'],
                                "to": addr[0]['to_date']
                            }
                        }
                    )
        else:
            user_addresses = self.helper.get_e_addresses(uuid)

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
            try:
                found_address = only(potential_matches)
                if found_address is not None:
                    types_to_edit[field] = found_address
            except ValueError:
                logger.warning('Multiple addresses found, not syncing for {}: {}'.format(uuid, field))
                continue
        logger.debug('Existing fields for {}: {}'.format(uuid, types_to_edit))
        return types_to_edit

    def _create_address(self, uuid, value, klasse):
        """Create a new address for a user.

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
        """Edit an exising address to a new value.

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
            user_engagements = self.helper.read_user_engagement(
                uuid, calculate_primary=True, read_all=True
            )
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

    def _create_it_system(self, person_uuid, ad_username, mo_itsystem_uuid):
        payload = {
            "type": "it",
            "user_key": ad_username,
            "itsystem": {"uuid": mo_itsystem_uuid},
            "person": {"uuid": person_uuid},
            "validity": VALIDITY
        }
        logger.debug("Create it system payload: {}".format(payload))
        response = self.helper._mo_post("details/create", payload)
        logger.debug("Response: {}".format(response.text))
        response.raise_for_status()

    def _update_it_system(self, ad_username, binding_uuid):
        payload = {
            "type": "it",
            "data": {
                "user_key": ad_username,
                "validity": VALIDITY
            },
            "uuid": binding_uuid,
        }
        logger.debug("Update it system payload: {}".format(payload))
        response = self.helper._mo_post("details/edit", payload)
        logger.debug("Response: {}".format(response.text))
        response.raise_for_status()

    def _edit_it_system(self, uuid, ad_object):
        mo_itsystem_uuid = self.mapping["it_systems"]["samAccountName"]
        if self.lc:
            it_systems = map(itemgetter(0), self.lc.it_connections.values())
            it_systems = filter(lambda it: it["user"] == uuid, it_systems)
            it_systems = filter(lambda it: it["itsystem"] == mo_itsystem_uuid, it_systems)
            it_systems = map(itemgetter("username", "uuid"), it_systems)
        else:
            it_systems = self.helper.get_e_itsystems(uuid, mo_itsystem_uuid)
            it_systems = map(itemgetter("user_key", "uuid"), it_systems)
        # Here it_systems is a 2 tuple (mo_username, binding_uuid)
        mo_username, binding_uuid = only(it_systems, ("", ""))
        # Username currently in AD
        ad_username = ad_object["SamAccountName"]

        # If mo_username is blank, we found a user who needs a new entry created
        if mo_username == "":
            self._create_it_system(uuid, ad_username, mo_itsystem_uuid)
            self.stats["it_systems"] += 1
            self.stats["users"].add(uuid)
        elif mo_username != ad_username:  # We need to update the mo_username
            self._update_it_system(ad_username, binding_uuid)
            self.stats["it_systems"] += 1
            self.stats["users"].add(uuid)

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

    def _finalize_it_system(self, uuid):
        if 'it_systems' not in self.mapping:
            return

        today = datetime.strftime(datetime.now(), "%Y-%m-%d")
        it_systems = {
            it['itsystem']['uuid']: it for it in
            self.helper.get_e_itsystems(uuid)
        }

        def check_validity_is_ok(uuid):
            # NOTE: Maybe this should be not set, or in the future?
            if not uuid in it_systems:
                return False
            return it_systems[uuid]['validity']['to'] is None

        # Find fields to terminate
        it_system_uuids = self.mapping['it_systems'].values()
        it_system_uuids = filter(check_validity_is_ok, it_system_uuids)

        for uuid in it_system_uuids:
            payload = {
                'type': 'it',
                'uuid': it_systems[uuid]["uuid"],
                'validity': {"to": today}
            }
            logger.debug('Finalize payload: {}'.format(payload))
            response = self.helper._mo_post('details/terminate', payload)
            logger.debug('Response: {}'.format(response.text))

    def _finalize_user_addresses(self, uuid):
        if 'user_addresses' not in self.mapping:
            return

        today = datetime.strftime(datetime.now(), "%Y-%m-%d")
        fields_to_edit = self._find_existing_ad_address_types(uuid)

        def check_field_in_fields_to_edit(field):
            return field in fields_to_edit.keys()

        def check_validity_is_ok(field):
            # NOTE: Maybe this should be not set, or in the future?
            return fields_to_edit[field]['validity']['to'] is None

        # Find fields to terminate
        address_fields = self.mapping['user_addresses'].keys()
        address_fields = filter(check_field_in_fields_to_edit, address_fields)
        address_fields = filter(check_validity_is_ok, address_fields)
        for field in address_fields:
            payload = {
                'type': 'address',
                'uuid': fields_to_edit[field]['uuid'],
                'validity': {"to": today}
            }
            logger.debug('Finalize payload: {}'.format(payload))
            response = self.helper._mo_post('details/terminate', payload)
            logger.debug('Response: {}'.format(response.text))

    def _terminate_single_user(self, uuid):
        self._finalize_it_system(uuid)
        self._finalize_user_addresses(uuid)

    def _update_single_user(self, uuid, ad_object, terminate_disabled):
        """Update all fields for a single user.

        :param uuid: uuid of the user.
        :param ad_object: Dict with the AD information for the user.
        """
        # Debug log if enabled is not found
        if 'Enabled' not in ad_object:
            logger.info("Enabled not in ad_object")
        user_enabled = ad_object.get('Enabled', True)

        # If terminate_disabled is None, we decide on a per-user basis using the
        # terminate_disabled_filters, by invariant we atleast one exist.
        if terminate_disabled is None:
            terminate_disabled = all(
                terminate_disabled_filter((uuid, ad_object))
                for terminate_disabled_filter in self.terminate_disabled_filters
            )

        # Check whether the current user is disabled, and terminate them, if we are
        # configured to terminate disabled users.
        if terminate_disabled and not user_enabled:
            # Set validity end --> today if in the future
            self._terminate_single_user(uuid)
            return

        # Sync the user, whether disabled or not
        if 'it_systems' in self.mapping:
            self._edit_it_system(uuid, ad_object)

        if 'engagements' in self.mapping:
            self._edit_engagement(uuid, ad_object)

        if 'user_addresses' in self.mapping:
            self._edit_user_addresses(uuid, ad_object)

    def _setup_ad_reader_and_cache_all(self, index):
        ad_reader = adreader.ADParameterReader(index=index)
        print('Retrive AD dump')
        ad_reader.cache_all()
        print('Done')
        logger.info('Done with AD caching')
        return ad_reader

    def _verify_it_systems(self):
        """Verify that all configured it-systems exist."""
        if 'it_systems' not in self.mapping:
            return

        # Set of UUIDs of all it_systems in MO
        mo_it_systems = set(
            map(itemgetter('uuid'), self.helper.read_it_systems())
        )

        @apply
        def filter_found(it_system, it_system_uuid):
            return it_system_uuid not in mo_it_systems

        # List of tuples (name, uuid) of it-systems configured in settings
        configured_it_systems = self.mapping['it_systems'].items()
        # Remove all the ones that exist in MO
        configured_it_systems = filter(filter_found, configured_it_systems)

        for it_system, it_system_uuid in configured_it_systems:
            msg = '{} with uuid {}, not found in MO'
            raise Exception(msg.format(it_system, it_system_uuid))

    def update_all_users(self):
        # Iterate over all AD's 
        for index, _ in enumerate(self.settings["integrations.ad"]):

            self.stats = {
                'ad-index': index,
                'addresses': [0, 0],
                'engagements': 0,
                'it_systems': 0,
                'users': set()
            }

            ad_reader = self._setup_ad_reader_and_cache_all(index=index)

            # move to read_conf_settings og valider på tværs af alle-ad'er
            # så vi ikke overskriver addresser, itsystemer og extensionfelter 
            # fra et ad med værdier fra et andet
            self.mapping = ad_reader._get_setting()['ad_mo_sync_mapping']
            self._verify_it_systems()

            used_mo_fields = []

            for key in self.mapping.keys():
                for ad_field, mo_combi in self.mapping.get(key, {}).items():
                    if mo_combi in used_mo_fields:
                        msg = 'MO field {} used more than once'
                        raise Exception(msg.format(mo_combi))
                    used_mo_fields.append(mo_combi)

            def employee_to_cpr_uuid(employee):
                """Convert an employee to a tuple (cpr, uuid)."""
                uuid = employee['uuid']
                if 'cpr' in employee:
                    cpr = employee['cpr']
                else:
                    cpr = self.helper.read_user(uuid)['cpr_no']
                return cpr, uuid

            @apply
            def cpr_uuid_to_uuid_ad(cpr, uuid):
                ad_object = ad_reader.read_user(cpr=cpr, cache_only=True)
                return uuid, ad_object

            @apply
            def filter_no_ad_object(uuid, ad_object):
                return ad_object

            # Lookup whether or not to terminate disabled users
            terminate_disabled = ad_reader._get_setting().get(
                "ad_mo_sync_terminate_disabled"
            )
            # If not globally configured, and no user filters are configured either,
            # we default terminate_disabled to False
            if terminate_disabled is None and not self.terminate_disabled_filters:
                terminate_disabled = False

            # Iterate over all users and sync AD informations to MO.
            employees = self._read_all_mo_users()
            employees = tqdm(employees)
            employees = map(employee_to_cpr_uuid, employees)
            employees = map(cpr_uuid_to_uuid_ad, employees)
            # Remove all entries without ad_object
            missing_employees, employees = partition(filter_no_ad_object, employees)
            # Run all pre filters
            for pre_filter in self.pre_filters:
                employees = filter(pre_filter, employees)
            # Call update_single_user on each remaining user
            for uuid, ad_object in employees:
                self._update_single_user(uuid, ad_object, terminate_disabled)
            # Call terminate on each missing user
            for uuid, ad_object in employees:
                self._terminate_single_user(uuid)

            logger.info('Stats: {}'.format(self.stats))
        self.stats['users'] = 'Written in log file'
        print(self.stats)


if __name__ == '__main__':
    ad_logger.start_logging('ad_mo_sync.log')

    sync = AdMoSync()
    sync.update_all_users()
