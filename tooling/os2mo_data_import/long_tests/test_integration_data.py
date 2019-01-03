#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
from freezegun import freeze_time
import sys
import random
import requests
import unittest
from datetime import datetime
from urllib.parse import urljoin

path_base = '/home/clint/os2mo-data-import-and-export/'
fixture_generator_path = path_base + 'tooling/fixture_generator'
exporters_path = path_base + 'exporters/'
import_path = path_base + 'tooling/os2mo_data_import/os2mo_data_import'
sys.path.append(import_path)
sys.path.append(exporters_path)
sys.path.append(fixture_generator_path)
from mora_helpers import MoraHelper
# from os2mo_data_import.data_types import Organisation
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

    def _find_top_unit(self):
        """ Find the imported uuid of 'Park og Vej' """
        org = self.morah.read_organisation()
        # Kan slettes?
        # org_uuid = self.morah._mo_lookup(org, 'o/', use_cache=False)[0]['uuid']
        units = self.morah._mo_lookup(org, 'o/{}/ou', use_cache=False)
        for unit in units['items']:
            if unit['name'] == 'Læsø Kommune':
                uuid = unit['uuid']
        return uuid

    def _count(self):
        counts = {}
        orgfunc = ('/organisation/organisationfunktion' +
                   '?gyldighed=Aktiv&funktionsnavn={}')
        unit = '/organisation/organisationenhed?gyldighed=Aktiv'
        user = '/organisation/bruger?bvn=%'

        url = urljoin(self.mox_base, orgfunc.format('Engagement'))
        response = requests.get(url)
        counts['engagement_count'] = len(response.json()['results'][0])

        url = urljoin(self.mox_base, orgfunc.format('Rolle'))
        response = requests.get(url)
        counts['role_count'] = len(response.json()['results'][0])

        url = urljoin(self.mox_base, orgfunc.format('Leder'))
        response = requests.get(url)
        counts['manager_count'] = len(response.json()['results'][0])

        url = urljoin(self.mox_base, orgfunc.format('Tilknytning'))
        response = requests.get(url)
        counts['association_count'] = len(response.json()['results'][0])

        url = urljoin(self.mox_base, unit)
        response = requests.get(url)
        counts['unit_count'] = len(response.json()['results'][0])

        url = urljoin(self.mox_base, user)
        response = requests.get(url)
        counts['person_count'] = len(response.json()['results'][0])
        return counts

    def _run_import_and_test_org_sanity(self, extra=0):
        dummy_import = ImportUtility(
            dry_run=False,
            mox_base=self.mox_base,
            mora_base=self.mora_base,
            store_integration_data=True,
            system_name=self.system_name
        )
        counts = self._count()
        dummy_import.import_all(self.dummy_org.org)
        counts = self._count()
        test_values = [
            ('role_count', 3),
            ('association_count', 2),
            ('engagement_count', 15),
            ('unit_count', 9 + extra),
            ('manager_count', 7),
            ('person_count', 18 + extra)
        ]
        for key, value in test_values:
            with self.subTest(key=key, value=value):
                self.assertEqual(counts[key], value)

    @freeze_time("2018-12-01")
    def test_010_correct_initial_import(self):
        """ Initial import and test that the org is as expected """
        self._run_import_and_test_org_sanity()

    @freeze_time("2018-12-01")
    def test_011_verify_existence_of_integration_data(self):
        """ Verify that integration data has been created """
        uuid = self._find_top_unit()
        integration_data = self.morah._mo_lookup(uuid, 'ou/{}/integration-data',
                                                 use_cache=False)
        self.assertTrue('integration_data' in integration_data)

    @freeze_time("2018-12-01")
    def test_012_verify_sane_integration_data(self):
        """ If integration data exists, verify that it has the expected content """
        uuid = self._find_top_unit()
        integration_data = self.morah._mo_lookup(uuid, 'ou/{}/integration-data',
                                                 use_cache=False)
        if 'integration_data' in integration_data:
            self.assertTrue(self.system_name in integration_data['integration_data'])
        else:
            self.skipTest('Integration data does not exist')

    @freeze_time("2018-12-01")
    def test_013_klasse_re_import(self):
        """ All classes should be imprted """
        org = self.morah.read_organisation()
        classes = self.morah._mo_lookup(org, 'o/{}/f/job_function/', use_cache=False)
        assert(len(classes['data']['items']) == 19)

    @freeze_time("2018-12-02")
    def test_020_re_import(self):
        """ Run the import again. This should result in an organisation of
        the same size. We also at the same time move a single user between
        two units. The success of this move is checked in a later test."""

        # TODO: This needs to work:
        """
        new_uuid = self._find_top_unit()
        print(new_uuid)
        karen = self.dummy_org.org.Employee.storage_map['KarenN']
        karen['optional_data'] = [
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
             ('value', 'karenn@laeso.dk ')],
            [('type', 'it'), ('user_key', 'KarenN'),
             ('itsystem', 'Office 365'),
             ('validity', {'to': None, 'from': '1997-01-16'})],
            [('type', 'it'), ('user_key', 'KarenN'),
             ('itsystem', 'Active Directory'),
             ('validity', {'to': None, 'from': '1997-01-16'})]
        ]
        self.dummy_org.org.Employee.storage_map['KarenN'] = karen
        """
        self._run_import_and_test_org_sanity()

    @freeze_time("2018-12-02")
    def test_021_klasse_re_import(self):
        """ No extra classes should be imprted after the second import """
        org = self.morah.read_organisation()
        classes = self.morah._mo_lookup(org, 'o/{}/f/job_function/', use_cache=False)
        assert(len(classes['data']['items']) == 19)

    @freeze_time("2018-12-02")
    def test_022_it_system_re_import(self):
        """ No extra itsystems should be imprted after the second import """
        service = urljoin(self.mox_base, '/organisation/itsystem?bvn=%')
        response = requests.get(service)
        response = response.json()
        assert(len(response['results'][0]) == 5)

    @freeze_time("2018-12-03")
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

    @freeze_time("2018-12-03")
    def test_031_test_forced_uuid(self):
        unit = self.morah._mo_lookup('00000000-0000-0000-0000-000000000001',
                                     'ou/{}/integration-data')
        self.assertTrue('name' in unit)

    @freeze_time("2018-12-03")
    def test_032_test_forced_employee_uuid(self):
        unit = self.morah._mo_lookup('00000000-0000-0000-0000-000000000002',
                                     'e/{}/integration-data')
        self.assertTrue('name' in unit)

    @freeze_time("2018-12-04")
    def test_040_correct_initial_import(self):
        """ Import and test that the org is as expected """
        self._run_import_and_test_org_sanity(extra=1)


if __name__ == '__main__':
    unittest.main()
