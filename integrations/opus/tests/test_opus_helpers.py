from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, call, patch
from parameterized import parameterized

import xmltodict
from hypothesis import given
from hypothesis.strategies import datetimes, dictionaries, text
from parameterized import parameterized_class

from integrations.opus.opus_diff_import import start_opus_diff
from integrations.opus import opus_helpers


class test_opus_helpers(TestCase):
    @given(text())
    def test_generate_uuid(self, value):
        uuid1 = opus_helpers.generate_uuid(value)
        uuid2 = opus_helpers.generate_uuid(value)
        self.assertEqual(uuid1, uuid2)

    @parameterized.expand(
        [
            (Path.cwd() / "integrations/opus/tests/ZLPETESTER_delta.xml",),
            (Path.cwd() / "integrations/opus/tests/ZLPETESTER2_delta.xml",),
        ]
    )
    def test_file_diff(self, file):
        filter_ids = []
        units, employees = opus_helpers.file_diff(file, file, filter_ids)
        self.assertEqual(units, [])
        self.assertEqual(employees, [])

    @parameterized.expand(
        [
            (Path.cwd() / "integrations/opus/tests/ZLPETESTER_delta.xml",),
            (Path.cwd() / "integrations/opus/tests/ZLPETESTER2_delta.xml",),
        ]
    )
    def test_parser(self, file):
        self.units, self.employees = opus_helpers.parser(file, [])
        self.assertIsInstance(self.units, list)
        self.assertIsInstance(self.employees, list)
        self.assertIsInstance(self.units[0], OrderedDict)
        self.assertIsInstance(self.employees[0], OrderedDict)


if __name__ == "__main__":
    unittest.main()
