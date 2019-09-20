import os
import logging
from datetime import datetime

import ad_reader
import ad_logger
from os2mo_helpers.mora_helpers import MoraHelper


logger = logging.getLogger('AdSyncRead')

MORA_BASE = os.environ.get('MORA_BASE')
VISIBLE = os.environ.get('VISIBLE_CLASS')
SECRET = os.environ.get('SECRET_CLASS')

if MORA_BASE is None:
    raise Exception('No address to MO indicated')

holstebro_mapping = {
    'user_addresses': {
        'mail': ('49b05fde-cb7a-6fb1-fcf5-59dae4bc647c', None),
        'mobile': ('f3abc4f2-c027-f514-a3ba-8cf11b53909a', VISIBLE),
        'physicalDeliveryOfficeName': ('377a83ab-57d4-9583-50c8-09753133b8c3', None),
        'telephoneNumber': ('0e5b0c70-0c71-a481-5712-7803d0b4cfa0', VISIBLE),
        'pager': ('f3abc4f2-c027-f514-a3ba-8cf11b53909a', SECRET)
    },
    'it_systems': {  # This are not par of AD->MO and could be removed.
        'samAccountName': 'aa76fa0e-3cf5-466c-bdaa-60d11d92cf7d'
    }
}

# AD has  no concept of temporality, validity is always from now to infinity.
VALIDITY = {
    'from':  datetime.strftime(datetime.now(), "%Y-%m-%d"),
    'to': None
}
ORG = {'uuid': '85c3f2fc-4af8-4fa0-b391-4cd54a244dcb'}


class AdMoSync(object):
    def __init__(self):
        logger.info('AD Sync Started')
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)

        found_visible = False
        found_secret = False
        for visibility in self.helper.read_classes_in_facet('visibility')[0]:
            if visibility['uuid'] == VISIBLE:
                found_visible = True
            if visibility['uuid'] == SECRET:
                found_secret = True
        if not (found_visible and found_secret):
            raise Exception('Error in visibility class configuration')

        self.ad_reader = ad_reader.ADParameterReader()
        self.ad_reader.cache_all()
        logger.info('Done with AD caching')

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
        org = self.helper.read_organisation()
        employee_list = self.helper._mo_lookup(org, 'o/{}/e?limit=1000000000')
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
        for field, klasse in holstebro_mapping['user_addresses'].items():
            found_address = None
            for address in user_addresses:
                if not address['address_type']['uuid'] == klasse[0]:
                    continue
                if klasse[1] is not None and 'visibility' in address:
                    if klasse[1] == address['visibility']['uuid']:
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
            'org': ORG  # Temporary, bug in MO.
        }
        if klasse[1] is not None:
            payload['visibility'] = {'uuid': klasse[1]}
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
            payload[0]['data']['visibility'] = {'uuid': klasse[1]}
        logger.debug('Edit payload: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        logger.debug('Response: {}'.format(response))

    def _update_single_user(self, uuid, ad_object):
        """
        Update all fields for a single user.
        :param uuid: uuid of the user.
        :param ad_object: Dict with the AD information for the user.
        """
        fields_to_edit = self._find_existing_ad_address_types(uuid)

        # Assume no existing adresses, we fix that later
        for field, klasse in holstebro_mapping['user_addresses'].items():
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
