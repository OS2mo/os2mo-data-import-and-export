#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import os
import unittest

from . integration_test_helpers import _count

from freezegun import freeze_time
from os2mo_data_import import ImportHelper
from os2mo_helpers.mora_helpers import MoraHelper
from integration_abstraction.integration_abstraction import IntegrationAbstraction

MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:5000')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')


class IntegrationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.mox_base = MOX_BASE
        self.mora_base = MORA_BASE
        self.system_name = 'Test Dummy Import'

        importer = ImportHelper(create_defaults=True,
                                mox_base=MOX_BASE,
                                mora_base=MORA_BASE,
                                system_name=self.system_name,
                                end_marker="STOP",
                                store_integration_data=True)

        importer.add_organisation('Org', user_key='Org', municipality_code=101)

        importer.add_klasse('Afdeling', facet_type_ref="org_unit_type")
        importer.add_klasse('Extra afdeling', facet_type_ref="org_unit_type")
        importer.add_klasse('Kok', facet_type_ref='engagement_job_function')
        importer.add_klasse('Vagt', facet_type_ref='engagement_job_function')
        importer.add_klasse('Ansat', facet_type_ref="engagement_type")
        importer.add_klasse('Orlov', facet_type_ref="leave_type")
        importer.add_klasse('Konsulent', facet_type_ref="association_type")
        importer.add_klasse('EAN', scope='EAN',
                            facet_type_ref='org_unit_address_type')
        importer.add_klasse('PhoneUnit', scope='PHONE',
                            facet_type_ref="org_unit_address_type")
        importer.import_all()

    @classmethod
    def setUp(self):
        self.morah = MoraHelper(use_cache=False)
        self.ia = IntegrationAbstraction(MOX_BASE, self.system_name, "STOP")
        self.importer = ImportHelper(create_defaults=True,
                                     mox_base=MOX_BASE,
                                     mora_base=MORA_BASE,
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
        Test that we are able to perform a simple import, including retreiving
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
            value='1234567890123',
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
            value='1234567890124',
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
            user_key='108',
            engagement_type_ref="Ansat",
            date_from='1992-01-23',
            date_to='2022-07-16'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Sub unit 1',
            job_function_ref='Vagt',
            user_key='109',
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

    def test_021_test_user_key_and_integration_data(self):
        """
        Test the user_key is correctly written.
        """
        engagement_list = self.morah.read_user_engagement(
            '00000000-0000-0000-1000-000000000000',
            at='2000-01-01'
        )
        
        job_ids = {'108', '109'}
        integration_data = {
            '5579606a75a36c085affcb6bebb1032ed628ee32a0a60ef5d4649bb63d68f9cd',
            '2adb36c907c188300808dedb0220fec7217cf901a64492eb0cfbd6ee86964534'
        }

        resource = 'organisation/organisationfunktion'
        for engagement in engagement_list:
            job_ids.remove(engagement['user_key'])
            uuid = engagement['uuid']
            integration = self.ia.read_integration_data(resource, uuid)
            integration_data.remove(integration)

        self.assertTrue(len(job_ids) == 0)
        self.assertTrue(len(integration_data) == 0)

    @freeze_time("2018-12-12")
    def test_022_test_length_of_double_engagements(self):
        """
        Check change of double engagement, length of one should be independent of
        change of the other
        """

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
        # Test the user_key is still in place

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

    @freeze_time("2018-12-25")
    def test_023_add_leave_to_user(self):
        """
        Add a leave to a user.
        Check that the engagement is not stopped when the leave stops.
        """
        print('Add leave!')
        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118'
        )

        self.importer.add_leave(
            employee='Test user',
            leave_type_ref='Orlov',
            date_from='2019-01-01',
            date_to='2030-01-01'
        )

        self.importer.import_all()
        count = _count(self.mox_base, at='1969-01-01')
        self.assertTrue(count['engagement_count'] == 0)

        count = _count(self.mox_base, at='2019-01-02')
        print(count)
        self.assertTrue(count['engagement_count'] == 2)
        self.assertTrue(count['leave_count'] == 1)

        count = _count(self.mox_base, at='2031-01-01')
        self.assertTrue(count['leave_count'] == 0)

    @freeze_time("2019-01-05")
    def test_024_change_leave(self):
        """
        Add a leave to a user.
        Check that the engagement is not stopped when the leave stops.
        """
        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118'
        )

        self.importer.add_leave(
            employee='Test user',
            leave_type_ref='Orlov',
            date_from='2019-01-01',
            date_to='2020-01-01'
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
        count = _count(self.mox_base, at='1969-01-01')
        self.assertTrue(count['engagement_count'] == 0)

        count = _count(self.mox_base, at='2019-01-02')
        print(count)
        self.assertTrue(count['engagement_count'] == 2)
        self.assertTrue(count['leave_count'] == 1)

        count = _count(self.mox_base, at='2021-01-02')
        print(count)
        self.assertTrue(count['engagement_count'] == 2)
        self.assertTrue(count['leave_count'] == 0)

    @freeze_time("2019-01-05")
    def test_025_integration_supported_association_and_role(self):
        """
        Add a role and and association to employees without adding the  units to the
        importer map.
        """
        self.importer.add_employee(
            name='Another test user',
            identifier='Another test user',
            cpr_no='1111111128',
        )
        self.importer.add_association(
            employee="Another test user",
            organisation_unit="Sub unit 1",
            association_type_ref="Konsulent",
            date_from="2018-12-25"
        )

        # Add association to existing employee
        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118',
        )

        self.importer.add_association(
            employee="Test user",
            organisation_unit="Sub unit 1",
            association_type_ref="Konsulent",
            date_from="2018-12-25"
        )

        self.importer.import_all()
        count = _count(self.mox_base, at='1969-01-01')
        self.assertTrue(count['association_count'] == 0)
        count = _count(self.mox_base, at='2019-01-10')
        self.assertTrue(count['association_count'] == 2)

    def test_026_terminate_employee(self):
        """
        Teriminate an employee
        """
        self.importer.add_employee(
            name='Test user',
            identifier='Test user',
            cpr_no='1111111118',
        )
        self.importer.terminate_employee(
            employee='Test user',
            date_from='2019-06-01',
        )

        self.importer.import_all()


if __name__ == '__main__':
    unittest.main()
