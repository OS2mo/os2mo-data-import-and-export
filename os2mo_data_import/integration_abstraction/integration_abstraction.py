import json
from requests import Session


class IntegrationAbstraction(object):

    def __init__(self, mox_base, system_name, end_marker='STOP'):
        if not mox_base[-1] == '/':
            mox_base = mox_base + '/'
        self.mox_base = mox_base
        self.system_name = system_name
        self.end_marker = end_marker
        self.session = Session()

    def _get_complete_object(self, resource, uuid):
        """ Return a complete LoRa object """
        response = self.session.get(url=self.mox_base + resource + '/' + uuid)
        response.raise_for_status()
        return response.json()

    def _get_attributes(self, resource, uuid):
        """ Return the 'Attributter' part of a LoRa object """
        mox_object = self._get_complete_object(resource, uuid)
        # How to handle multiple 'registreringer'?
        attributes = mox_object[uuid][0]['registreringer'][0]['attributter']
        return attributes

    def _get_integration_data(self, resource, uuid):
        """
        Return the the raw integration data string, no interpretation
        is performed, except for a validation of the data as valid json
        :param resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid:
        uuid of the object to be returned.
        :return: Raw integration data string.
        """
        attributes = self._get_attributes(resource, uuid)
        for key in attributes.keys():
            if key.find('egenskaber') > 0:
                data = attributes[key][0].get('integrationsdata', None)
        if data is not None:
            try:
                json.loads(data)
            except json.decoder.JSONDecodeError:
                raise Exception('Invalid json in integration data')
        return data

    def _set_integration_data(self, resource, uuid, data):
        """
        Updates or creates a raw integration data string in LoRa.
        This is a helper function that will do no attempts of preserving
        existing data. The resulting integration data string is guaranted to
        be valid json.
        :param resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid: uuid of the object to be updated.
        :return: Return none if data is unchanged, otherwise the uuid of the
        object is returned.
        """
        attributter = self._get_attributes(resource, uuid)
        for key in attributter.keys():
            if key.find('egenskaber') > 0:
                current = attributter[key][0].get('integrationsdata', None)
                attributter[key][0]['integrationsdata'] = data
        properties = {'attributter': attributter}

        # Here we should verify that the data is valid json and check
        # whether it is different from the current value
        if current == data:
            return None
        response = self.session.patch(url=self.mox_base + resource +
                                      '/' + uuid, json=properties)
        response.raise_for_status()
        return response.json()

    def read_integration_data(self, resource, uuid):
        """
        Returns the integration data (if any) with the relevant system name.
        :param  resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid: uuid of the object.
        :return: Integration data associated with self.system_name.
        """
        return_value = None

        integration_data = self._get_integration_data(resource, uuid)
        if integration_data is not None:
            structured_data = json.loads(integration_data)
            data = structured_data.get(self.system_name, '')
            end_pos = data.find(self.end_marker)
            if end_pos > -1:
                return_value = json.loads(data[0:end_pos])
        return return_value

    def integration_data_payload(self, resource, value, uuid=None, encode=True):
        """
        Return payload for integration data after update of the relavant system.
        This payload can be included in a bigger payload.
        :param  resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid: uuid of the object.
        :param value: New integration data value.
        """
        if uuid:
            integration_data_string = self._get_integration_data(resource, uuid)
        if uuid and integration_data_string:
            integration_data = json.loads(integration_data_string)
        else:
            integration_data = {}

        value_string = '{}{}'.format(json.dumps(value), self.end_marker)
        integration_data[self.system_name] = value_string
        if encode:
            integration_data_string = json.dumps(integration_data)
        else:
            integration_data_string = integration_data
        return integration_data_string

    def write_integration_data(self, resource, uuid, value):
        """
        Write new integration data for current system.name. If data is already
        present, it will be overwritten.
        :param  resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid: uuid of the object.
        :param value: New integration data value.
        """
        integration_data_string = self._get_integration_data(resource, uuid)
        if integration_data_string is not None:
            integration_data = json.loads(integration_data_string)
        else:
            integration_data = {}

        value_string = '{}{}'.format(json.dumps(value), self.end_marker)

        integration_data[self.system_name] = value_string
        integration_data_string = json.dumps(integration_data)
        self._set_integration_data(resource, uuid, integration_data_string)
        return True

    def find_object(self, resource, key):
        url = self.mox_base + resource + '?integrationsdata=%25{}%25'

        # key_string = repr(key[1:-1]) + self.end_marker
        key_string = json.dumps(key) + self.end_marker
        search_val = json.dumps({self.system_name: key_string})
        search_val = search_val[1:-1]  # Remove { and }
        search_string = search_val.replace('\\', '\\\\')

        response = self.session.get(url=url.format(search_string))
        response.raise_for_status()
        results = response.json()['results'][0]
        if len(results) == 0:
            return_val = None
        elif len(results) == 1:
            return_val = results[0]
        else:
            raise Exception('Inconsistent integration data!')
        return return_val


if __name__ == '__main__':
    ia = IntegrationAbstraction(mox_base='http://localhost:8080',
                                system_name='test',
                                end_marker='Jørgen')

    test_integration_data = json.dumps({"test": "12345Jørgen",
                                        "system": "98Jør\\gen"})

    resource = '/klassifikation/facet'
    uuid = '645e9050-0cad-4138-96b2-6dc89dbdce01'
    print(ia._get_complete_object(resource, uuid))
    print(ia._set_integration_data(resource, uuid, test_integration_data))
    # print(ia.read_integration_data(resource, uuid))
    # print(ia.find_object(resource, '1234'))
    # print(ia.write_integration_data(resource, uuid, '123'))
