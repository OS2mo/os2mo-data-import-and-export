import json
from requests import Session


class IntegrationAbstraction(object):

    def __init__(self, mox_base, system_name, end_marker=None):
        self.mox_base = mox_base
        self.system_name = system_name
        self.end_marker = end_marker
        self.session = Session()

    def _test_payload(self):
        properties = {
            "attributter": {
                "facetegenskaber": [
                    {
                        'integrationsdata': 'Test',
                        'virkning': {
                            "from": "2014-05-22 12:02:32",
                            "to": "infinity", 
                            }
                    }
                ]
            }
        }
        return properties
        
    def _get_complete_object(self, resource, uuid):
        response = self.session.get(url=self.mox_base + resource + '/' + uuid)
        return response.json()

    def _get_attributes(self, resource, uuid):
        mox_object = self._get_complete_object(resource, uuid)
        # How to handle multiple 'registreringer'?
        attributes = mox_object[uuid][0]['registreringer'][0]['attributter']
        return attributes

    def _get_integration_data(self, resource, uuid):
        attributes = self._get_attributes(resource, uuid)
        for key in attributes.keys():
            if key.find('egenskaber') > 0:
                data = attributes[key][0].get('integrationsdata', None)
        return data

    def _set_integration_data(self, resource, uuid, data):
        """ Updates or creates a raw integrationsdata string in LoRa.
        This is a helper function that will do no attempts of preserving
        existing data.
        :param resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid: uuid of the object to be updated
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
        return response.json()

    def read_integration_data(self, resource, uuid):
        """ Returns the integratio data (if any) with the relevant system name
        :param  resource:
        Path of the service endpoint (str) e.g. /organisation/organisation
        :param uuid: uuid of the object
        :return: Integration data associated with self.system_name
        """
        return_value = None
        
        integration_data = self._get_integration_data(resource, uuid)
        if integration_data is not None:
            structured_data = json.loads(integration_data)
            data = structured_data.get(self.system_name, '')
            end_pos = data.find(self.end_marker)
            if end_pos > -1:
                return_value = data[0:end_pos]
        return return_value

if __name__ == '__main__':
    ia = IntegrationAbstraction(mox_base='http://localhost:8080',
                                system_name='test',
                                end_marker='Jørgen')

    test_integration_data = json.dumps({"test": "1234Jørgen", "system": "98Jørgen"})
    
    resource = '/klassifikation/facet'
    uuid = '645e9050-0cad-4138-96b2-6dc89dbdce01'
    #print(ia._get_complete_object(resource, uuid))
    #print(ia._set_integration_data(resource, uuid, test_integration_data))
    print(ia.read_integration_data(resource, uuid))
