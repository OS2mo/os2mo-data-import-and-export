import sqlite3
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

from parameterized import parameterized

from integrations.SD_Lon.db_overview import DBOverview

sqlite3.connect = MagicMock(name='sqlite3.connect')


class Test_db_overview(unittest.TestCase):
    def setUp(self):
        self.db_overview = DBOverview()

    @parameterized.expand([
        [(datetime.now(), 'Lorem Ipsum'), (True, 'Status ok')],
        [(datetime.now() - timedelta(days=1), 'Lorem Ipsum'), (False, 'Not up to date')],
        [(datetime.now(), 'Running'), (False, 'Not ready to run')],
    ])
    def test_read_current_status(self, fixture_row, expected):
        sqlite3.connect.return_value.cursor.return_value.fetchone.return_value = fixture_row
        status, message = self.db_overview.read_current_status()
        expected_status, expected_message = expected
        self.assertEqual(status, expected_status)
        self.assertEqual(message, expected_message)


if __name__ == '__main__':
    unittest.main()
