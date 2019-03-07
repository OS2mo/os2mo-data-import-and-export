#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import unittest

from . integration_test_helpers import _count

from freezegun import freeze_time
from os2mo_data_import import ImportHelper
from os2mo_helpers.mora_helpers import MoraHelper


class IntegrationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.mox_base = 'http://localhost:5000'
        self.mora_base = 'http://localhost:80'
        self.system_name = 'Test Dummy Import'

        importer = ImportHelper(create_defaults=True,
                                mox_base='http://localhost:5000',
                                mora_base='http://localhost:80',
                                system_name=self.system_name,
                                end_marker="STOP",
                                store_integration_data=True)

        importer.add_organisation(identifier='Org', user_key='Org',
                                  municipality_code=101)

        importer.add_klasse(identifier='Afdeling',
                            facet_type_ref="org_unit_type")

        importer.add_klasse(identifier='Extra afdeling',
                            facet_type_ref="org_unit_type")

        importer.add_klasse(identifier='EAN', scope='TEXT',
                            facet_type_ref='org_unit_address_type')

        importer.add_klasse(identifier="PhoneUnit", scope='TEXT',
                            facet_type_ref="org_unit_address_type")

        importer.add_klasse(identifier='Kok', scope='TEXT',
                            facet_type_ref='engagement_job_function')

        importer.add_klasse(identifier='Vagt', scope='TEXT',
                            facet_type_ref='engagement_job_function')

        importer.add_klasse(identifier='Ansat', scope='TEXT',
                            facet_type_ref="engagement_type")

        importer.import_all()

    @classmethod
    def setUp(self):
        self.morah = MoraHelper(use_cache=False)
        self.importer = ImportHelper(create_defaults=True,
                                     mox_base='http://localhost:5000',
                                     mora_base='http://localhost:80',
                                     system_name=self.system_name,
                                     end_marker="STOP",
                                     store_integration_data=True)

        self.importer.add_organisation(identifier='Org', user_key='Org',
                                       municipality_code=101)

    @classmethod
    def tearDownClass(self):
        pass

    @freeze_time("2018-12-05")
    def test_010_import_simple_org(self):
        """
        Test that we are able to perform a simple impor, including retreiving
        class information from integration data.
        """

        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000001',
            date_from='1970-01-01'
        )

        self.importer.add_address_type(
            organisation_unit='Root',
            value='11111111',
            type_ref='EAN',
            date_from='1970-01-01',
        )

        self.importer.add_address_type(
            organisation_unit='Root',
            value='11111111',
            type_ref='PhoneUnit',
            date_from='1970-01-01',
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            parent_ref='Root',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000011',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Root',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000021',
            date_from='1970-01-01'
        )

        self.importer.add_address_type(
            organisation_unit='Sub unit 2',
            value='33333333',
            type_ref='EAN',
            date_from='1970-01-01',
        )

        self.importer.add_address_type(
            organisation_unit='Sub unit 2',
            value='33333333',
            type_ref='PhoneUnit',
            date_from='1970-01-01',
        )

        self.importer.import_all()
        count = _count(self.mox_base, at='1969-01-01')
        self.assertTrue(count['unit_count'] == 0)

        count = _count(self.mox_base)
        self.assertTrue(count['unit_count'] == 3)

    @freeze_time("2018-12-06")
    def test_011_re_import_simple_org(self):
        """
        Integration data should ensure nothing changes
        """
        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )
        self.importer.import_all()
        count = _count(self.mox_base)
        self.assertTrue(count['unit_count'] == 3)

    def test_012_import_without_uuids(self):
        """
        Test the units and uuids forced in test 010 are kept on re-import
        """

        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )
        self.importer.import_all()
        unit = self.morah._mo_lookup('00000000-0000-0000-0000-000000000001',
                                     'ou/{}/integration-data')
        self.assertTrue('name' in unit)

    @freeze_time("2018-12-07")
    def test_013_rename_unit(self):
        """
        Test that a rename returns old and new name on suitable dates
        """
        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            name='Sub unit 1.1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )
        self.importer.import_all()
        unit = self.morah.read_ou(
            '00000000-0000-0000-0000-000000000011', at='2018-12-06')
        self.assertTrue(unit['name'] == 'Sub unit 1')

        unit = self.morah.read_ou(
            '00000000-0000-0000-0000-000000000011', at='2018-12-08')
        self.assertTrue(unit['name'] == 'Sub unit 1.1')

    @freeze_time("2018-12-08")
    def test_014_move_unit(self):

        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            name='Sub unit 1.1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Sub unit 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
        )

        self.importer.import_all()
        unit = self.morah.read_ou(
            '00000000-0000-0000-0000-000000000021', at='2018-12-07')
        self.assertTrue(unit['parent']['name'] == 'Root')

        unit = self.morah.read_ou(
            '00000000-0000-0000-0000-000000000021', at='2018-12-09')
        self.assertTrue(unit['parent']['name'] == 'Sub unit 1.1')

    @freeze_time("2018-12-09")
    def test_015_change_address(self):
        """
        Importer updates either all or nothing, so in this case only
        the changed address should be valid after the update.
        """
        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            name='Sub unit 1.1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Sub unit 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
        )

        self.importer.add_address_type(
            organisation_unit="Sub unit 2",
            value='33333334',
            type_ref='PhoneUnit',
            date_from='2018-12-09',
        )

        self.importer.import_all()
        address = self.morah._mo_lookup('00000000-0000-0000-0000-000000000021',
                                        'ou/{}/details/address',
                                        at='2018-12-08')
        self.assertTrue(len(address) == 2)

        address = self.morah._mo_lookup('00000000-0000-0000-0000-000000000021',
                                        'ou/{}/details/address',
                                        at='2018-12-10')
        self.assertTrue(len(address) == 1)
        self.assertTrue(address[0]['value'] == '33333334')

    @freeze_time("2018-12-15")
    def test_016_integration_supported_import(self):
        """
        Import a unit without its dependencies. The dependent units should be read
        from integration data.
        """
        self.importer.add_organisation_unit(
            identifier='Sub unit 3',
            parent_ref='Sub unit 2',
            type_ref='Extra Afdeling',
            date_from='2018-12-15'
        )
        self.importer.import_all()
        count = _count(self.mox_base, at='1969-01-01')
        self.assertTrue(count['unit_count'] == 0)

        count = _count(self.mox_base)
        self.assertTrue(count['unit_count'] == 4)
        address = self.morah._mo_lookup('00000000-0000-0000-0000-000000000021',
                                        'ou/{}/details/address',
                                        at='2018-12-10')
        self.assertTrue(len(address) == 1)
        self.assertTrue(address[0]['value'] == '33333334')

    @freeze_time("2018-12-15")
    def test_017_prepare_stress_test(self):
        self.importer.add_organisation_unit(
            identifier='Sub unit 9',
            parent_ref=None,
            type_ref='Afdeling',
            date_from='2018-12-15'
        )
        for i in range(10, 25):
            self.importer.add_organisation_unit(
                identifier='Sub unit {}'.format(i),
                parent_ref='Sub unit {}'.format(i - 1),
                type_ref='Afdeling',
                date_from='2018-12-15'
            )
        self.importer.import_all()
        count = _count(self.mox_base)
        self.assertTrue(count['unit_count'] == 20)

    @freeze_time("2018-12-15")
    def test_018_stress_integration_supported_import(self):
        """
        Import a unit without its dependencies. The dependent units and the
        type class should be read from integration data.
        """
        self.importer.add_organisation_unit(
            identifier='Sub unit 25',
            parent_ref='Sub unit 24',
            type_ref='Afdeling',
            date_from='2018-12-15'
        )
        self.importer.import_all()
        count = _count(self.mox_base)
        self.assertTrue(count['unit_count'] == 21)

    @freeze_time("2018-12-10")
    def test_020_double_engagements(self):
        """
        Add two engagements to a single user and check they are correct.
        When importer is able to read-back integration data, the unit
        creation can be removed.
        """
        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            name='Sub unit 1.1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Sub unit 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
        )

        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118',
            uuid='00000000-0000-0000-1000-000000000000'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Root',
            job_function_ref='Kok',
            engagement_type_ref="Ansat",
            date_from='1992-01-23',
            date_to='2022-07-16'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Sub unit 1',
            job_function_ref='Vagt',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to=None
        )
        self.importer.import_all()
        count = _count(self.mox_base, at='1980-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 0)

        count = _count(self.mox_base, at='1991-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 1)

        count = _count(self.mox_base, at='1993-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 2)

        count = _count(self.mox_base, at='2023-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 1)

    @freeze_time("2018-12-12")
    def test_021_test_length_of_double_engagements(self):
        """
        Check change of double engagement, length of one should be independent of
        change of the other
        """
        self.importer.add_organisation_unit(
            identifier='Root',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 1',
            name='Sub unit 1.1',
            parent_ref='Root',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Sub unit 2',
            parent_ref='Sub unit 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
        )

        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118',
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Root',
            job_function_ref='Kok',
            engagement_type_ref="Ansat",
            date_from='1992-01-23',
            date_to='2022-07-16'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Sub unit 1',
            job_function_ref='Vagt',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to='2020-07-16'
        )
        self.importer.import_all()
        count = _count(self.mox_base, at='1980-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 0)

        count = _count(self.mox_base, at='1991-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 1)

        count = _count(self.mox_base, at='1993-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 2)

        count = _count(self.mox_base, at='2021-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 1)

        count = _count(self.mox_base, at='2023-01-01')
        self.assertTrue(count['person_count'] == 1)
        self.assertTrue(count['engagement_count'] == 0)


if __name__ == '__main__':
    unittest.main()
