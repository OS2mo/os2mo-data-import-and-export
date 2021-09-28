from copy import deepcopy
import unittest
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import xmltodict
from hypothesis import given
from hypothesis.strategies import datetimes, dictionaries, text
from parameterized import parameterized, parameterized_class

from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import start_opus_diff

testfile1 = Path.cwd() / "integrations/opus/tests/ZLPETESTER_delta.xml"
testfile2 = Path.cwd() / "integrations/opus/tests/ZLPETESTER2_delta.xml"


class test_opus_helpers(TestCase):
    @given(text())
    def test_generate_uuid(self, value):
        uuid1 = opus_helpers.generate_uuid(value)
        uuid2 = opus_helpers.generate_uuid(value)
        self.assertEqual(uuid1, uuid2)

    @parameterized.expand(
        [
            (testfile1,),
            (testfile2,),
        ]
    )
    def test_file_diff_same(self, file):
        units, employees = opus_helpers.file_diff(file, file, disable_tqdm=True)
        self.assertEqual(units, [])
        self.assertEqual(employees, [])

    def test_file_diff(self):
        units, employees = opus_helpers.file_diff(testfile1, testfile2, disable_tqdm=True)
        self.assertNotEqual(units, [])
        self.assertNotEqual(employees, [])

    @parameterized.expand(
        [
            (testfile1,),
            (testfile2,),
        ]
    )
    def test_parser(self, file):
        self.units, self.employees = opus_helpers.parser(file)
        self.assertIsInstance(self.units, list)
        self.assertIsInstance(self.employees, list)
        self.assertIsInstance(self.units[0], OrderedDict)
        self.assertIsInstance(self.employees[0], OrderedDict)

    @parameterized.expand(
        [
            (testfile1, [], set()),
            (testfile2, [], set()),
            (testfile1, ["1"], {"1", "2", "3"}),
            (testfile2, ["1"], {"1", "2", "3", "4"}),
        ]
    )
    def test_all_filtered_ids_fiter_top_unit(self, file, filter_ids, expected):
        all_filtered = opus_helpers.find_all_filtered_ids(file, filter_ids)
        self.assertEqual(len(all_filtered), len(expected))
        self.assertEqual(all_filtered, expected)

    @parameterized.expand(
        [
            (testfile1, 1),
            (testfile2, 1),
        ]
    )
    def test_split_employees_active(self, inputfile, expected):
        filter_ids = ["1"]
        all_filtered_ids = opus_helpers.find_all_filtered_ids(inputfile, filter_ids)
        units, employees = opus_helpers.file_diff(None, inputfile, disable_tqdm=True)
        emplyees, leaves = opus_helpers.split_employees_leaves(employees)
        employees = list(employees)
        leaves = list(leaves)
        self.assertEqual(len(leaves), expected)

    @parameterized.expand(
        [
            ([], [3, 0, 3, 1]),
            (["1"], [0, 3, 0, 1]),
        ]
    )
    def test_full_(self, filter_ids, expected):
        data = opus_helpers.read_and_transform_data(None, testfile1, filter_ids)
        for i, x in enumerate(data):
            this = list(x)
            l = len(this)
            self.assertEqual(l, expected[i])
    
    
    def test_find_changed_parent(self):
        org1, _ = opus_helpers.parser(testfile1)
        org2 = deepcopy(org1)
        org2[2]['parentOrgUnit'] = "CHANGED!"
        diffs = opus_helpers.find_changes(before=org1, after=org2, disable_tqdm=True)
        self.assertEqual(diffs, [org2[2]])


if __name__ == "__main__":
    unittest.main()
