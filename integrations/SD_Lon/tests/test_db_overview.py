import unittest
from unittest.mock import MagicMock, patch
from integrations.SD_Lon.db_overview import DBOverview
from datetime import date, datetime, timedelta
import sqlite3
sqlite3.connect = MagicMock(name='sqlite3.connect')

class Test_db_overview(unittest.TestCase):

    def test_db_call(self):
        db_overview = DBOverview()
        db_overview.read_db_content()
        sqlite3.connect.assert_called()
    
    def test_date_now(self):
        db_overview = DBOverview()
        sqlite3.connect.return_value.cursor.return_value.fetchone.return_value = (datetime.now(),'Lorem Ipsum')
        status, msg = db_overview.read_current_status()
        self.assertTrue(status)
        self.assertEqual(msg,'Status ok')
        
    def test_date_yesterday(self):
        db_overview = DBOverview()
        sqlite3.connect.return_value.cursor.return_value.fetchone.return_value = (datetime.now()-timedelta(days=1),'Lorem Ipsum')
        status, msg = db_overview.read_current_status()
        self.assertFalse(status)
        self.assertEqual(msg,'Not up to date')

    def test_running(self):
        db_overview = DBOverview()
        sqlite3.connect.return_value.cursor.return_value.fetchone.return_value = (datetime.now(),'Running')
        status, msg = db_overview.read_current_status()
        self.assertFalse(status)
        self.assertEqual(msg,'Not ready to run')

if __name__ == '__main__':
    unittest.main()