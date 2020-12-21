import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

from integrations.SD_Lon.db_overview import DBOverview
from parameterized import parameterized


class Test_db_overview(unittest.TestCase):
    def setUp(self):
        self.db_overview = DBOverview({"integrations.SD_Lon.import.run_db": "dummy"})

    @parameterized.expand(
        [
            [(datetime.now(), "Lorem Ipsum"), (True, "Status ok")],
            [
                (datetime.now() - timedelta(days=1), "Lorem Ipsum"),
                (False, "Not up to date"),
            ],
            [(datetime.now(), "Running"), (False, "Not ready to run")],
        ]
    )
    @patch("integrations.SD_Lon.db_overview.sqlite3", autospec=True)
    def test_read_current_status(self, fixture_row, expected, sqlite3_mock):
        sqlite3_mock.connect.return_value.cursor.return_value.fetchone.return_value = (
            fixture_row
        )
        status, message = self.db_overview.read_current_status()
        expected_status, expected_message = expected
        self.assertEqual(status, expected_status)
        self.assertEqual(message, expected_message)


if __name__ == "__main__":
    unittest.main()
