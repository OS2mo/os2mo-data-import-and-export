import json
import pickle
import random
import unittest
import requests
from integration_abstraction.integration_abstraction import (
    IntegrationAbstraction
)


class IntegratioAbstractionTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.session = requests.Session()
        self.mox_base = 'http://localhost:8080'
        self.resource = '/klassifikation/facet'
        self.uuids = ['00000000-0000-0000-0000-000000000001',
                      '00000000-0000-0000-0000-000000000002',
                      '00000000-0000-0000-0000-000000000003',
                      '00000000-0000-0000-0000-000000000004',
                      '00000000-0000-0000-0000-000000000005']

    def setUp(self):
        with open('tests/facet_opret.json') as f:
            payload = json.load(f)

        service = self.mox_base + self.resource
        for uuid in self.uuids:
            self.session.put(url=service + '/' + uuid, json=payload)

    def test_raw_write(self):
        """ Test that we can write a litteral string and read a key back """
        ia = IntegrationAbstraction(self.mox_base, 'test', 'Jørgen')
        test_integration_data = json.dumps(
            {"test": json.dumps(12345) + "Jørgen",
             "system": json.dumps("98") + "Jør\\gen"}
        )
        ia._set_integration_data(self.resource, self.uuids[0],
                                 test_integration_data)

        key = ia.read_integration_data(self.resource, self.uuids[0])
        self.assertTrue(key == 12345)

    def test_field_write(self):
        ia = IntegrationAbstraction(self.mox_base, 'system', 'Jørgen')
        test_integration_data = json.dumps({"test": "12345Jørgen",
                                            "system": "98Jør\\gen"})
        ia._set_integration_data(self.resource, self.uuids[1],
                                 test_integration_data)
        set_key = '1'
        ia.write_integration_data(self.resource, self.uuids[1], set_key)
        read_key = ia.read_integration_data(self.resource, self.uuids[1])
        self.assertTrue(set_key == read_key)

    def test_writing_complicated_value(self):
        ia = IntegrationAbstraction(self.mox_base, 'system', 'Jørgen')
        set_key = 'kμl!%a/h\&#/##=)=&"rf'
        ia.write_integration_data(self.resource, self.uuids[2], set_key)
        read_key = ia.read_integration_data(self.resource, self.uuids[2])
        self.assertTrue(set_key == read_key)

    def test_find_simple_value(self):
        key = 'Klaff'
        ia = IntegrationAbstraction(self.mox_base, 'simpel', 'STOP')

        # Check we find the key
        ia.write_integration_data(self.resource, self.uuids[3], key)
        found_uuid = ia.find_object(self.resource, key)
        self.assertTrue(found_uuid == self.uuids[3])

        # Check we do not find a wrong key
        found_uuid = ia.find_object(self.resource, key[:-1])
        self.assertFalse(found_uuid)

    def test_find_complex_value(self):
        """ Check that we can find a value with non-trival characters.
        This does not mean tha we can find everything, searchigh for
        characters also used as escapes (?, #, &) will not work"""
        key = 'abc"¤μ)(d'
        ia = IntegrationAbstraction(self.mox_base, 'compløx', 'Jørgen')

        # Check we find the key
        ia.write_integration_data(self.resource, self.uuids[3], key)
        found_uuid = ia.find_object(self.resource, key)
        self.assertTrue(found_uuid == self.uuids[3])

        # Check we do not find a wrong key
        found_uuid = ia.find_object(self.resource, key[:-1])
        self.assertFalse(found_uuid)

    def test_nested_value(self):
        """ Values can be nested, but they need to string keys """
        ia = IntegrationAbstraction(self.mox_base, 'system', 'Jørgen')
        set_key = {'a': 2, 'b': 3, 'c': {'a': 1, 'b': 2, '5': {'def': 9}}}
        ia.write_integration_data(self.resource, self.uuids[2], set_key)
        read_key = ia.read_integration_data(self.resource, self.uuids[2])
        self.assertTrue(set_key == read_key)

    def test_pickle_save(self):
        """ Values can be nested, but they need to string keys """
        ia = IntegrationAbstraction(self.mox_base, 'system', 'Jørgen')
        data = []
        for i in range(0, 100):
            data.append(random.random())
        set_key = pickle.dumps(data)
        ia.write_integration_data(self.resource, self.uuids[1], set_key)
        
        read_key = ia.read_integration_data(self.resource, self.uuids[1])
        read_data = pickle.loads(read_key)
        self.assertTrue(read_data == data)
