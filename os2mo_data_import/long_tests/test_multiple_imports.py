#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import unittest

from os2mo_data_import import ImportHelper
from freezegun import freeze_time
from os2mo_helpers.mora_helpers import MoraHelper


class IntegrationDataTests(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.morah = MoraHelper()
        self.mox_base = 'http://localhost:5000'
        self.mora_base = 'http://localhost:80'
        self.system_name = 'Test Dummy Import'
        self.importer = ImportHelper(create_defaults=True,
                                     mox_base='http://localhost:5000',
                                     mora_base='http://localhost:80',
                                     system_name=self.system_name,
                                     end_marker="STOP",
                                     store_integration_data=True)

        self.importer.add_organisation(identifier='Org', user_key='Org',
                                       municipality_code=101)

        self.importer.add_klasse(identifier='Afdeling',
                                 facet_type_ref="org_unit_type")

        # self.importer.add_klasse(identifier='AddressMailUnit', scope='TEXT',
        #                          facet_type_ref='org_unit_address_type')

        self.importer.add_klasse(identifier="PhoneUnit", scope='TEXT',
                                 facet_type_ref="org_unit_address_type")

        self.importer.add_klasse(identifier='EAN', scope='TEXT',
                                 facet_type_ref='org_unit_address_type')

        self.importer.add_klasse(identifier='Kok', scope='TEXT',
                                 facet_type_ref='engagement_job_function')

        self.importer.add_klasse(identifier='Vagt', scope='TEXT',
                                 facet_type_ref='engagement_job_function')

        self.importer.add_klasse(identifier='Ansat', scope='TEXT',
                                 facet_type_ref="engagement_type")

    @classmethod
    def tearDownClass(self):
        pass

    @freeze_time("2018-12-05")
    def ttest_010_import_simple_org(self):

        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            uuid='00000000-0000-0000-0000-000000000001',
            date_from='1970-01-01'
        )

        self.importer.add_address_type(
            organisation_unit="Test enhed",
            value="11111111",
            type_ref='EAN',
            date_from='1970-01-01',
        )

        self.importer.add_address_type(
            organisation_unit="Test enhed",
            value="11111111",
            type_ref='PhoneUnit',
            date_from='1970-01-01',
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000011',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000021',
            date_from='1970-01-01'
        )

        self.importer.add_address_type(
            organisation_unit="Test underenhed 2",
            value="33333333",
            type_ref='EAN',
            date_from='1970-01-01',
        )

        self.importer.add_address_type(
            organisation_unit="Test underenhed 2",
            value="33333333",
            type_ref='PhoneUnit',
            date_from='1970-01-01',
        )

        self.importer.import_all()

    @freeze_time("2018-12-06")
    def ttest_011_re_import_simple_org(self):

        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            uuid='00000000-0000-0000-0000-000000000001',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000011',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            name='Test underenhed 2',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            uuid='00000000-0000-0000-0000-000000000021',
            date_from='1970-01-01'
        )

        self.importer.import_all()

    @freeze_time("2018-12-06")
    def ttest_012_import_without_uuids(self):
        """ Integration data should ensure nothing changes """

        self.importer.add_organisation_unit(
            identifier='Test enhed',
            name='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            name='Test underenhed 2',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )

        self.importer.import_all()

    @freeze_time("2018-12-07")
    def ttest_013_rename_unit(self):
        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1 med nyt navn',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='1970-01-01'
        )

        self.importer.import_all()

    @freeze_time("2018-12-08")
    def ttest_014_move_unit(self):

        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1 med nyt navn',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            parent_ref='Test underenhed 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
        )

        self.importer.import_all()

    @freeze_time("2018-12-08")
    def ttest_015_change_address(self):
        """
        Importer updates either all or nothing, so in this case only
        the changed address should be valid after the update.
        """
        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1 med nyt navn',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            parent_ref='Test underenhed 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
        )

        self.importer.add_address_type(
            organisation_unit="Test underenhed 2",
            value="33333334",
            type_ref='PhoneUnit',
            date_from='2018-12-08',
        )

        self.importer.import_all()

    @freeze_time("2018-12-10")
    def test_020_double_engagements(self):
        """
        Add two engagements to a single user and check they are correct.
        When importer is able to read-back integration data, the unit
        creation can be removed.
        """
        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1 med nyt navn',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            parent_ref='Test underenhed 1',
            type_ref='Afdeling',
            date_from='2018-12-08'
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
            job_function_ref='Kok',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to='2022-07-16'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Test underenhed 2',
            job_function_ref='Vagt',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to=None
        )
        self.importer.import_all()

    @freeze_time("2018-12-12")
    def test_021_test_length_of_double_engagements(self):
        """
        Check change of double engagement, length of one should be independent of
        change of the other
        """
        self.importer.add_organisation_unit(
            identifier='Test enhed',
            parent_ref=None,
            type_ref="Afdeling",
            date_from='1970-01-01'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 1',
            name='Test underenhed 1 med nyt navn',
            parent_ref='Test enhed',
            type_ref='Afdeling',
            date_from='2018-12-07'
        )

        self.importer.add_organisation_unit(
            identifier='Test underenhed 2',
            parent_ref='Test underenhed 1',
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
            organisation_unit='Test enhed',
            job_function_ref='Kok',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to='2022-07-16'
        )

        self.importer.add_engagement(
            employee='Test user',
            organisation_unit='Test underenhed 2',
            job_function_ref='Vagt',
            engagement_type_ref="Ansat",
            date_from='1990-01-23',
            date_to='2020-07-16'
        )
        self.importer.import_all()


if __name__ == '__main__':
    unittest.main()
