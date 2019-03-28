#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import os
import random
import requests
import unittest
from datetime import datetime
from urllib.parse import urljoin
from freezegun import freeze_time

from . integration_test_helpers import _count

from os2mo_helpers.mora_helpers import MoraHelper
from os2mo_data_import import ImportHelper
from fixture_generator.populate_mo import CreateDummyOrg
from fixture_generator.populate_mo import Size

MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:5000')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')


class IntegrationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        random.seed(1)
        self.morah = MoraHelper(use_cache=False)
        self.mox_base = MOX_BASE
        self.mora_base = MORA_BASE
        self.system_name = 'Test Dummy Import'
        self.importer = ImportHelper(create_defaults=True,
                                     mox_base=self.mox_base,
                                     mora_base=self.mora_base,
                                     system_name=self.system_name,
                                     end_marker="STOP",
                                     store_integration_data=True)
        self.dummy_org = CreateDummyOrg(self.importer, 825, 'Læsø Kommune',
                                        scale=1, org_size=Size.Small,
                                        extra_root=False)

    @classmethod
    def setUp(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass

    def _find_top_unit(self):
        """ Find the imported uuid of 'Park og Vej' """
        org = self.morah.read_organisation()
        units = self.morah._mo_lookup(org, 'o/{}/ou', use_cache=False)
        for unit in units['items']:
            if unit['name'] == 'Læsø Kommune':
                uuid = unit['uuid']
        return uuid

    def _run_import_and_test_org_sanity(self, extra_unit=0, extra_employee=0,
                                        extra_engagement=0):
        self.importer.import_all()
        counts = _count(self.mox_base)
        test_values = [
            ('role_count', 5),
            ('association_count', 4),
            ('engagement_count', 14 + extra_engagement),
            ('unit_count', 9 + extra_unit),
            ('manager_count', 4),
            ('person_count', 20 + extra_employee)
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
        classes = self.morah._mo_lookup(org, 'o/{}/f/engagement_job_function/',
                                        use_cache=False)
        self.assertTrue(len(classes['data']['items']) == 18)

    @freeze_time("2018-12-02")
    def test_014_test_engagement_from_date(self):
        """ Write a test that verifies that engagements of a certain age exists """
        self.assertTrue(True)

    @freeze_time("2018-12-02")
    def test_020_re_import(self):
        """
        Run the import again. This should result in an organisation of the same size.
        We also at the same time move a single user between two units. The success of
        this move is checked in a later test.
        """
        self._run_import_and_test_org_sanity()

    @freeze_time("2018-12-02")
    def test_021_klasse_re_import(self):
        """ No extra classes should be imprted after the second import """
        org = self.morah.read_organisation()
        classes = self.morah._mo_lookup(org, 'o/{}/f/engagement_job_function/',
                                        use_cache=False)
        self.assertTrue(len(classes['data']['items']) == 18)

    @freeze_time("2018-12-02")
    def test_022_it_system_re_import(self):
        """ No extra itsystems should be imprted after the second import """
        service = urljoin(self.mox_base, '/organisation/itsystem?bvn=%')
        response = requests.get(service)
        response = response.json()
        self.assertTrue(len(response['results'][0]) == 5)

    def test_23_test_engagement_from_date(self):
        """
        Verifies that engagements of a certain age and length exists, this ensures
        that we did not accidentially cut off all engagements"""
        count = _count(self.mox_base, at='1969-01-01')
        self.assertTrue(count['engagement_count'] > 0)

        count = _count(self.mox_base, at='2220-01-01')
        self.assertTrue(count['engagement_count'] > 0)

    @freeze_time("2018-12-03")
    def test_030_add_forced_uuids(self):
        """ Add a unit, employees and classes with forced uuid, and re-import """
        self.importer.add_organisation_unit(
            identifier='Test enhed',
            name='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            uuid='00000000-0000-0000-0000-000000000001',
            date_from=datetime.strftime(self.dummy_org.data.global_start_date,
                                        '%Y-%m-%d')
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000011',
            date_from=datetime.strftime(self.dummy_org.data.global_start_date,
                                        '%Y-%m-%d')
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            name='Test underenhed 2',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000021',
            date_from=datetime.strftime(self.dummy_org.data.global_start_date,
                                        '%Y-%m-%d')
        )

        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118',
            uuid='00000000-0000-0000-0000-000000000002'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Test enhed',
            job_function_ref='Udvikler',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to='2022-07-16'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Test underenhed 2',
            job_function_ref='Ergoterapeut',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to=None
        )

        self._run_import_and_test_org_sanity(
            extra_unit=3,
            extra_employee=1,
            extra_engagement=2
        )

    @freeze_time("2018-12-03")
    def test_031_test_forced_uuid(self):
        unit = self.morah._mo_lookup('00000000-0000-0000-0000-000000000001',
                                     'ou/{}/integration-data', use_cache=False)
        self.assertTrue('name' in unit)

    @freeze_time("2018-12-03")
    def test_032_test_forced_employee_uuid(self):
        person = self.morah._mo_lookup('00000000-0000-0000-0000-000000000002',
                                       'e/{}/integration-data', use_cache=False)
        self.assertTrue('name' in person)


if __name__ == '__main__':
    unittest.main()
