#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import datetime
import os
import sys
import unittest

import psycopg2
import sd_importer  # noqa
from integration_test_helpers import _count  # noqa
from os2mo_data_import import ImportHelper
from os2mo_helpers.mora_helpers import MoraHelper

sys.path.append("../SD_Lon")

sys.path.append("../../os2mo_data_import/long_tests")

MUNICIPALTY_NAME = os.environ.get("MUNICIPALITY_NAME")
MUNICIPALTY_CODE = os.environ.get("MUNICIPALITY_CODE")
MOX_BASE = os.environ.get("MOX_BASE", "http://localhost:5000")
MORA_BASE = os.environ.get("MORA_BASE", "http://localhost:80")
GLOBAL_GET_DATE = datetime.datetime(2019, 6, 13, 0, 0)


class IntegrationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.morah = MoraHelper(use_cache=False)

    def _clear_database(self):
        conn = psycopg2.connect(
            user="mox", dbname="mox", host="localhost", password="mox"
        )
        cursor = conn.cursor()

        query = (
            "select relname from pg_class where relkind='r' "
            + "and relname !~ '^(pg_|sql_)';"
        )

        cursor.execute(query)
        for row in cursor.fetchall():
            query = "truncate {} cascade;".format(row[0])
            cursor.execute(query)
        conn.commit()

    def setUp(self):
        self.importer = ImportHelper(
            create_defaults=True,
            mox_base=MOX_BASE,
            mora_base=MORA_BASE,
        )

    @classmethod
    def tearDownClass(self):
        pass

    def test_010_standard_import(self):
        """
        self a standard SD import
        """
        print("Test 010")
        self._clear_database()
        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix=None,
        )

        sd.create_ou_tree(create_orphan_container=True)
        self.importer.import_all()

        count = _count(MOX_BASE)
        print(count)
        self.assertTrue(count["unit_count"] in range(550, 560))

    def test_011_test_kommunaldirektør(self):
        print("Test 011")
        kommunaldirektør = self.morah.read_ou("3e890083-589b-4a00-ba00-000001350001")
        self.assertFalse("error_key" in kommunaldirektør)

    def test_012_test_udgåede(self):
        print("Test 012")
        udgåede = self.morah.read_ou("8df2c488-8038-4b00-a600-000001460003")
        self.assertFalse("error_key" in udgåede)

    def test_013_test_orphan(self):
        print("Test 013")
        org = self.morah.read_organisation()
        top_units = self.morah.read_top_units(org)
        orphans = 0
        for unit in top_units:
            if unit["name"] == "Forældreløse enheder":
                orphans += 1
        self.assertTrue(orphans == 1)

        # Forældreløse, Kommunalbestyrelsen, Kommnaldirektør, udgåede
        self.assertTrue(len(top_units) == 4)

    def test_020_standard_import(self):
        """
        Test a standard SD import, withoug grouping of orphan units
        """
        print("Test 020")
        self._clear_database()
        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix=None,
        )

        sd.create_ou_tree(create_orphan_container=False)

        self.importer.import_all()
        count = _count(MOX_BASE)
        print(count)
        self.assertTrue(count["unit_count"] in range(550, 555))

    def test_021_test_orphan(self):
        print("Test 021")
        org = self.morah.read_organisation()
        top_units = self.morah.read_top_units(org)
        orphans = 0
        for unit in top_units:
            if unit["name"] == "Forældreløse enheder":
                orphans += 1
        self.assertTrue(orphans == 0)

        self.assertTrue(len(top_units) > 6)

    def test_030_import_subtree(self):
        """
        Test an import of a single sub-tree.
        """
        print("Test 030")
        self._clear_database()
        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix=None,
        )

        sd.create_ou_tree(
            create_orphan_container=False,
            # Direktør m.a. CAY,CBK,CTM, CEI
            sub_tree="3e890083-589b-4a00-ba00-000001350001",
        )

        self.importer.import_all()
        count = _count(MOX_BASE)
        print(count)
        self.assertTrue(count["unit_count"] in range(460, 465))

    def test_031_test_roots(self):
        print("Test 031")
        org = self.morah.read_organisation()
        top_units = self.morah.read_top_units(org)
        self.assertTrue(len(top_units) == 1)

    def test_040_import_subtree_with_super(self):
        """
        Test an import of a single sub-tree with a super unit.
        """
        print("Test 040")
        self._clear_database()
        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix=None,
        )

        adm_name = "AdmOrg"
        self.importer.add_klasse(
            identifier=adm_name,
            facet_type_ref="org_unit_type",
            user_key=adm_name,
            scope="TEXT",
            title=adm_name,
        )

        self.importer.add_organisation_unit(
            identifier=adm_name,
            name=adm_name,
            user_key=adm_name,
            type_ref=adm_name,
            date_from="1930-01-01",
            date_to=None,
            parent_ref=None,
        )

        sd.create_ou_tree(
            create_orphan_container=False,
            super_unit=adm_name,
            sub_tree="3e890083-589b-4a00-ba00-000001350001",
        )

        self.importer.import_all()
        count = _count(MOX_BASE)
        print(count)
        self.assertTrue(count["unit_count"] in range(450, 470))

    def test_041_test_top_units(self):
        print("Test 041")
        org = self.morah.read_organisation()
        top_units = self.morah.read_top_units(org)
        super_unit = 0
        for unit in top_units:
            if unit["name"] == "AdmOrg":
                super_unit += 1
        self.assertTrue(super_unit == 1)

        self.assertTrue(len(top_units) == 1)

    def test_050_import_subtree_with_prefix(self):
        """
        Test an import of bvn-prefixed units.
        """
        print("Test 050")
        self._clear_database()
        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix="adm_",
        )

        sd.create_ou_tree(
            create_orphan_container=False,
            # 'Direktionen encoded with org prefix 'adm_'
            sub_tree="fff9e2a6-d670-b656-c719-994eeac03a74",
        )
        self.importer.import_all()

        count = _count(MOX_BASE)
        print(count)
        self.assertTrue(count["unit_count"] in range(450, 470))

    def test_051_test_prefix(self):
        print("Test 510")
        org = self.morah.read_organisation()
        top_units = self.morah.read_top_units(org)
        bvn = top_units[0]["user_key"]
        self.assertTrue(bvn[0:4] == "adm_")

    def test_060_full_double_import(self):
        # Import of Administration
        adm_name = "AdmOrg"
        print("Test 060")
        self._clear_database()

        self.importer.add_klasse(
            identifier=adm_name,
            facet_type_ref="org_unit_type",
            user_key=adm_name,
            scope="TEXT",
            title=adm_name,
        )

        self.importer.add_organisation_unit(
            identifier=adm_name,
            uuid="11111111-1111-1111-1111-111111111111",
            name=adm_name,
            user_key=adm_name,
            type_ref=adm_name,
            date_from="1930-01-01",
            date_to=None,
            parent_ref=None,
        )

        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix="adm_",
        )

        sd.create_ou_tree(
            create_orphan_container=False,
            # 'Direktionen encoded with org prefix 'adm_'
            sub_tree="fff9e2a6-d670-b656-c719-994eeac03a74",
            super_unit=adm_name,
        )

        # Import of Lønorganisation
        løn_name = "LønOrg"

        self.importer.add_klasse(
            identifier=løn_name,
            facet_type_ref="org_unit_type",
            user_key=løn_name,
            scope="TEXT",
            title=løn_name,
        )

        self.importer.add_organisation_unit(
            identifier=løn_name,
            uuid="22222222-2222-2222-2222-222222222222",
            name=løn_name,
            user_key=løn_name,
            type_ref=løn_name,
            date_from="1930-01-01",
            date_to=None,
            parent_ref=None,
        )

        sd = sd_importer.SdImport(
            self.importer,
            MUNICIPALTY_NAME,
            MUNICIPALTY_CODE,
            import_date_from=GLOBAL_GET_DATE,
            org_only=True,
            org_id_prefix=None,
        )

        sd.create_ou_tree(create_orphan_container=False, super_unit=løn_name)
        self.importer.import_all()
        count = _count(MOX_BASE)
        print(count)
        self.assertTrue(count["unit_count"] in range(1000, 1050))

    def test_061_test_top_units(self):
        print("Test 061")
        org = self.morah.read_organisation()
        top_units = self.morah.read_top_units(org)
        self.assertTrue(len(top_units) == 2)

    def test_062_test_kommunaldirektør(self):
        print("Test 062")
        kommunaldirektør = self.morah.read_ou("3e890083-589b-4a00-ba00-000001350001")
        self.assertFalse("error_key" in kommunaldirektør)

    def test_063_test_udgåede(self):
        print("Test 63")
        udgåede = self.morah.read_ou("8df2c488-8038-4b00-a600-000001460003")
        self.assertFalse("error_key" in udgåede)


if __name__ == "__main__":
    unittest.main()
