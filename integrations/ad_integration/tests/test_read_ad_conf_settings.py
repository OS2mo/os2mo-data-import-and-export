import sys
import unittest
from os.path import dirname

sys.path.append(dirname(__file__) + "/..")
sys.path.append(dirname(__file__) + "/../../..")

import read_ad_conf_settings


class TestReadADConfSettings(unittest.TestCase):
    def test_lazy_dict_initializer_is_called(self):
        with self.assertRaises(Exception) as context:
            read_ad_conf_settings.SETTINGS.get("invalid_key")
        self.assertTrue("No setting file" in str(context.exception))
