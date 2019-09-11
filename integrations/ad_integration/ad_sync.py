import os
from datetime import datetime

import ad_reader
from os2mo_helpers.mora_helpers import MoraHelper


MORA_BASE = os.environ.get('MORA_BASE', None)

# Specific to Holstebro - should go to conf
VISIBLE = 'da56b8e1-9069-b530-511f-23f1624c83eb'
SECRET = 'f0fa7f46-d8ce-d74b-4af6-ce989e6a034c'

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
        self.helper = MoraHelper(hostname=MORA_BASE, use_cache=False)
        self.ad_reader = ad_reader.ADParameterReader()
        # self.ad_info_reader.cache_all()

    def _read_mo_classes(self):
        # This is not really needed, unless we want to make a consistency check.
        emp_adr_classes = self.helper.read_classes_in_facet('employee_address_type')
        for emp_adr_class in emp_adr_classes[0]:
            print(emp_adr_class)

    def _read_all_mo_users(self):
        org = self.helper.read_organisation()
        employee_list = self.helper._mo_lookup(org, 'o/{}/e')
        employees = employee_list['items']
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
                    if klasse[1] is not None:
                        if klasse[1] == address['visibility']['uuid']:
                            found_address_type = address['uuid']
                    else:
                        found_address_type = address['uuid']
            if found_address_type is not None:
                types_to_edit[field] = found_address_type
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
        print(payload)
        response = self.helper._mo_post('details/create', payload)
        print(response)

    def _edit_address(self, address_uuid, value, klasse):
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
        print(payload)
        response = self.helper._mo_post('details/edit', payload)
        print(response)

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
                print('No such AD field: {}'.format(field))

    def update_all_users(self):
        employees = self._read_all_mo_users()

        for employee in employees:
            user = self.helper.read_user(employee['uuid'])
            cpr = user['cpr_no'][0:6] + '-' + user['cpr_no'][6:10]
            response = self.ad_reader.uncached_read_user(cpr=cpr)
            if response:
                # print(employee['uuid'])
                # for key, value in response.items():
                #   print('{}: {}'.format(key, value))
                self._update_single_user(employee['uuid'], response)


if __name__ == '__main__':
    sync = AdMoSync()
    # sync._read_mo_classes()

    sync.update_all_users()
