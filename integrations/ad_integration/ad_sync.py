import os
import logging
from datetime import datetime

import ad_reader
from os2mo_helpers.mora_helpers import MoraHelper

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'ad_to_mo_sync.log'


logger = logging.getLogger('AdSyncRead')
detail_logging = ('AdSyncRead', 'AdReader', 'mora-helper')
for name in logging.root.manager.loggerDict:
    if name in detail_logging:
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


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
    'it_systems': {
        'samAccountName': 'aa76fa0e-3cf5-466c-bdaa-60d11d92cf7d'
    }
}

# AD has  no concept of temporalty, validity is always from now to infinity.
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
        # This is not really needed, unless we want to make a consistency check.
        emp_adr_classes = self.helper.read_classes_in_facet('employee_address_type')
        for emp_adr_class in emp_adr_classes[0]:
            print(emp_adr_class)

    def _read_all_mo_users(self):
        logger.info('Read all MO users')
        org = self.helper.read_organisation()
        employee_list = self.helper._mo_lookup(org, 'o/{}/e?limit=1000000000')
        employees = employee_list['items']
        logger.info('Done reading all MO users')
        return employees

    def _find_existing_ad_address_types(self, uuid):
        # user_addresses = self.helper.read_user_address(uuid)
        # Unfortunately, mora-helper currently does not read all addresses
        types_to_edit = {}
        user_addresses = self.helper._mo_lookup(uuid, 'e/{}/details/address')
        for field, klasse in holstebro_mapping['user_addresses'].items():
            found_address_type = None
            for address in user_addresses:
                if address['address_type']['uuid'] == klasse[0]:
                    if klasse[1] is not None and 'visibility' in address:
                        if klasse[1] == address['visibility']['uuid']:
                            found_address_type = address['uuid']
                    else:
                        found_address_type = address['uuid']
            if found_address_type is not None:
                types_to_edit[field] = found_address_type
        logger.debug('Existing fields for {}: {}'.format(uuid, types_to_edit))
        return types_to_edit

    def _create_address(self, uuid, value, klasse):
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
        logger.debug
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
        # types_to_edit contains a dict pairing AD field with existing MO objects
        fields_to_edit = self._find_existing_ad_address_types(uuid)

        # Assume no existing adresses, we fix that later
        for field, klasse in holstebro_mapping['user_addresses'].items():
            print()
            if field in ad_object:
                if field in fields_to_edit.keys():
                    self._edit_address(fields_to_edit[field],
                                       ad_object[field],
                                       klasse)
                else:
                    self._create_address(uuid, ad_object[field], klasse)
            else:
                logger.debug('No such AD field: {}'.format(field))

    def update_all_users(self):
        i = 0
        employees = self._read_all_mo_users()

        for employee in employees:
            i = i + 1
            print('Progress: {}/{}'.format(i, len(employees)))
            logger.info('Start sync of {}'.format(employee['uuid']))
            user = self.helper.read_user(employee['uuid'])
            # cpr = user['cpr_no'][0:6] + '-' + user['cpr_no'][6:10]
            cpr = user['cpr_no']
            # response = self.ad_reader.uncached_read_user(cpr=cpr)
            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)

            if response:
                self._update_single_user(employee['uuid'], response)
            logger.info('End sync of {}'.format(employee['uuid']))


if __name__ == '__main__':
    sync = AdMoSync()
    # sync._read_mo_classes()

    sync.update_all_users()
