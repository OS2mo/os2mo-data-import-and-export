#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import sys
import random
import requests
import unittest
fixture_generator_path = '/home/clint/os2mo-data-import-and-export/tooling/fixture_generator'
exporters_path = '/home/clint/os2mo-data-import-and-export/exporters/'
import_path = '/home/clint/os2mo-data-import-and-export/tooling/os2mo_data_import/os2mo_data_import'
sys.path.append(import_path)
sys.path.append(exporters_path)
sys.path.append(fixture_generator_path)
from datetime import datetime
from urllib.parse import urljoin
from mora_helpers import MoraHelper

from os2mo_data_import.data_types import Organisation
from os2mo_data_import.utility import ImportUtility
from populate_mo import CreateDummyOrg

class IntegrationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        random.seed(1)
        self.morah = MoraHelper()
        self.mox_base = 'http://localhost:8080'
        self.mora_base = 'http://localhost:80'
        self.system_name = 'Test Dummy Import'
        self.dummy_org = CreateDummyOrg(825, 'Læsø Kommune', scale=1,
                                        heavy_data_set=False, small_set=True)

    @classmethod
    def setUp(self):
        pass
        
    @classmethod
    def tearDownClass(self):
        pass

    def _find_park_og_vej(self):
        """ Find the imported uuid of 'Park og Vej' """
        org = self.morah.read_organisation()
        org_uuid = self.morah._mo_lookup(org, 'o/', use_cache=False)[0]['uuid']
        units = self.morah._mo_lookup(org, 'o/{}/ou', use_cache=False)
        park_og_vej = units['items'][4]
        uuid = park_og_vej['uuid']
        return uuid

    def _run_import_and_test_org_sanity(self, extra=0):
        dummy_import = ImportUtility(
            dry_run=False,
            mox_base=self.mox_base,
            mora_base=self.mora_base,
            store_integration_data=True,
            system_name=self.system_name
        )

        dummy_import.import_all(self.dummy_org.org)
        org = self.morah.read_organisation()
        counts = self.morah._mo_lookup(org, 'o/{}/', use_cache=False)
        test_values = [
            ('role_count', 3),
            ('association_count', 5),
            ('engagement_count', 21),
            ('unit_count', 12 + extra),
            ('manager_count', 10),
            ('person_count', 29 + extra)
        ]
        for key, value in test_values:
            with self.subTest(key=key, value=value):
                self.assertEqual(counts[key], value)
    
    def test_010_correct_initial_import(self):
        """ Initial import and test that the org is as expected """
        self._run_import_and_test_org_sanity()

    def test_011_verify_existence_of_integration_data(self):
        """ Verify that integration data has been created """
        uuid = self._find_park_og_vej()
        integration_data = self.morah._mo_lookup(uuid, 'ou/{}/integration-data',
                                                 use_cache=False)
        self.assertTrue('integration_data' in integration_data)

    def test_012_verify_sane_integration_data(self):
        """ If integration data exists, verify that it has the expected content """
        uuid = self._find_park_og_vej()
        integration_data = self.morah._mo_lookup(uuid, 'ou/{}/integration-data',
                                                 use_cache=False)
        if 'integration_data' in integration_data:
            self.assertTrue(self.system_name in integration_data['integration_data'])
        else:
            self.skipTest('Integration data does not exist')

    def test_013_klasse_re_import(self):
        """ All classes should be imprted """
        org = self.morah.read_organisation()
        classes = self.morah._mo_lookup(org, 'o/{}/f/job_function/', use_cache=False)
        assert(len(classes['data']['items']) == 19)

    def test_020_re_import(self):
        """ Run the import again. This should result in an organisation of
        the same size. We also at the same time move a single user between
        two units. The success of this move is checked in a later test."""

        new_uuid = self._find_park_og_vej()
        print(new_uuid)
        henny = self.dummy_org.org.Employee.storage_map['HennyR']
        henny['optional_data'] = [
            [('type', 'engagement'),
             ('org_unit', new_uuid),
             ('job_function', 'Udviklingskonsulent'),
             ('engagement_type', 'Ansat'),
             ('validity', {'to': None, 'from': '1997-01-16'})],
            [('type', 'address'),
             ('address_type', 'AdressePost'),
             ('validity', {'to': None, 'from': '1997-01-16'}),
             ('uuid', '919eb449-41e0-4290-b3c7-98cb83a652f9')],
            [('type', 'address'), ('address_type', 'Telefon'),
             ('validity', {'to': None, 'from': '1997-01-16'}),
             ('value', '46571200')],
            [('type', 'address'),
             ('address_type', 'Email'),
             ('validity', {'to': None, 'from': '1997-01-16'}),
             ('value', 'hennyr@laeso.dk')],
            [('type', 'it'), ('user_key', 'HennyR'),
             ('itsystem', 'Office 365'),
             ('validity', {'to': None, 'from': '1997-01-16'})],
            [('type', 'it'), ('user_key', 'HennyR'),
             ('itsystem', 'Active Directory'),
             ('validity', {'to': None, 'from': '1997-01-16'})],
            [('type', 'manager'),
             ('org_unit', new_uuid),
             ('manager_type', 'Direktør'),
             ('manager_level', 'Niveau 4'),
             ('responsibility', ['Personale: ansættelse/afskedigelse',
                                 'Personale: øvrige administrative opgaver',
                                 'Personale: Sygefravær']),
             ('validity', {'to': None, 'from': '1997-01-16'})]
        ]
        self.dummy_org.org.Employee.storage_map['HennyR']['optional_data'] = henny

        self._run_import_and_test_org_sanity()

    def test_021_klasse_re_import(self):
        """ No extra classes should be imprted after the second import """
        org = self.morah.read_organisation()
        classes = self.morah._mo_lookup(org, 'o/{}/f/job_function/', use_cache=False)
        assert(len(classes['data']['items']) == 19)

    def test_022_it_system_re_import(self):
        """ No extra itsystems should be imprted after the second import """
        service = urljoin(self.mox_base, '/organisation/itsystem?bvn=%')
        response = requests.get(service)
        response = response.json()
        assert(len(response['results'][0]) == 5)

    def test_030_add_forced_uuids(self):
        """ Add a unit, employees and classes with forced uuid, and re-import """
        self.dummy_org.org.OrganisationUnit.add(
            identifier='Test enhed',
            name='Test enhed',
            parent_ref=None,
            org_unit_type_ref="Afdeling",
            uuid='00000000-0000-0000-0000-000000000001',
            date_from=datetime.strftime(self.dummy_org.data.global_start_date,
                                        '%Y-%m-%d')
        )
        self.dummy_org.org.Employee.add(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118',
            uuid='00000000-0000-0000-0000-000000000002'
        )
        self._run_import_and_test_org_sanity(extra=1)

    def test_031_test_forced_uuid(self):
        unit = self.morah._mo_lookup('00000000-0000-0000-0000-000000000001',
                                     'ou/{}/integration-data')
        self.assertTrue('name' in unit)

    def test_032_test_forced_employee_uuid(self):
        unit = self.morah._mo_lookup('00000000-0000-0000-0000-000000000002',
                                     'e/{}/integration-data')
        self.assertTrue('name' in unit)
        
if __name__ == '__main__':
    unittest.main()
