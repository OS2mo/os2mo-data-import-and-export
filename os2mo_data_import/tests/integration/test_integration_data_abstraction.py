import os
import json
import random
import unittest
import requests
from integration_abstraction import IntegrationAbstraction

MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:5000')


class IntegratioAbstractionTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.session = requests.Session()
        self.mox_base = 'http://localhost:8080/'
        self.resource = 'klassifikation/facet'
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
            r = self.session.put(url=service + '/' + uuid, json=payload)
            print(r.status_code, r.url)

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
        set_value = '1'
        ia.write_integration_data(self.resource, self.uuids[1], set_value)
        read_value = ia.read_integration_data(self.resource, self.uuids[1])
        self.assertTrue(set_value == read_value)

    def test_writing_complicated_value(self):
        ia = IntegrationAbstraction(self.mox_base, 'system', 'Jørgen')
        set_value = 'kμl!%a/h\&#/##=)=&"rf'
        ia.write_integration_data(self.resource, self.uuids[2], set_value)
        read_value = ia.read_integration_data(self.resource, self.uuids[2])
        self.assertTrue(set_value == read_value)

    def test_find_simple_value(self):
        """
        Test that we can find a simple value by searchig for the value
        """
        value = 'Klaff'
        ia = IntegrationAbstraction(self.mox_base, 'simpel', 'STOP')

        # Check we find the key
        ia.write_integration_data(self.resource, self.uuids[3], value)
        found_uuid = ia.find_object(self.resource, value)
        self.assertTrue(found_uuid == self.uuids[3])

        # Check we do not find a wrong key
        found_uuid = ia.find_object(self.resource, value[:-1])
        self.assertFalse(found_uuid)

    def test_find_complex_value(self):
        """
        Check that we can find a value with non-trival characters.
        This does not mean that we can find everything, searchig for
        characters also used as escapes (?, #, &) will not work
        """
        value = 'abc"¤μ)(d'
        ia = IntegrationAbstraction(self.mox_base, 'compløx', 'Jørgen')

        # Check we find the key
        ia.write_integration_data(self.resource, self.uuids[3], value)
        found_uuid = ia.find_object(self.resource, value)
        self.assertTrue(found_uuid == self.uuids[3])

        # Check we do not find a wrong key
        found_uuid = ia.find_object(self.resource, value[:-1])
        self.assertFalse(found_uuid)

    def test_nested_value(self):
        """
        Values can be nested, but they need to have string keys
        """
        ia = IntegrationAbstraction(self.mox_base, 'system', 'Jørgen')
        set_value = {'a': 2, 'b': 3, 'c': {'a': 1, 'b': 2, '5': {'def': 9}}}
        ia.write_integration_data(self.resource, self.uuids[2], set_value)
        read_value = ia.read_integration_data(self.resource, self.uuids[2])
        self.assertTrue(set_value == read_value)

    def _prepare_many_systems(self, uuids):
        system_names = {'System', 'SD', 'ML-Gore', 'LØN', 'Løn', 'μ-system',
                        'Black', 'White', '-', 'klaf', 'bang', 'integration',
                        'Integration', 'INTEGRATION', '1', '2', '3', '4', '5'
                        '6', '101', '1962', '1961', '1981', '1982', '1983',
                        '1984', '1986', '1987', '1990', '1993', '1997', '2001',
                        '2005', '2009', '2013', '2017', 'WE', 'WANT', 'MÅER'}
        ias = {}
        for system in system_names:
            uuid = random.choice(uuids)
            value = system + str(random.randint(1, 999999))
            ias[system] = IntegrationAbstraction(self.mox_base, system)
            ias[system].write_integration_data(self.resource, uuid, value)
        return system_names, ias

    def test_many_keys(self):
        """
        Verify that we can have a number of keys and be able to read them all.
        """
        system_names, ias = self._prepare_many_systems(self.uuids)

        visited_systems = set()
        for uuid in self.uuids:
            for system in system_names:
                value = ias[system].read_integration_data(self.resource, uuid)
                if value is not None:
                    prefix = value[0:len(system)]
                    nummeric = int(value[len(system):])
                    self.assertTrue(prefix == system)
                    self.assertTrue(1 <= nummeric <= 999999)

                    self.assertTrue(system not in visited_systems)
                    visited_systems.add(system)
        self.assertTrue(visited_systems == system_names)

    def test_many_writes(self):
        """
        Verify that we can have a number of keys -  write extensively to one of
        them and still have them all intact.
        """
        uuid = self.uuids[0]
        system_names, ias = self._prepare_many_systems([uuid])

        system = system_names.pop()
        system_names.add(system)  # The system should stay in the list
        for i in range(0, 150):
            value = system + str(random.randint(1, 999999))
            ias[system].write_integration_data(self.resource, uuid, value)

        visited_systems = set()
        for system in system_names:
            value = ias[system].read_integration_data(self.resource, uuid)
            if value is not None:
                prefix = value[0:len(system)]
                nummeric = int(value[len(system):])
                self.assertTrue(prefix == system)
                self.assertTrue(1 <= nummeric <= 999999)

                self.assertTrue(system not in visited_systems)
                visited_systems.add(system)
        self.assertTrue(visited_systems == system_names)
