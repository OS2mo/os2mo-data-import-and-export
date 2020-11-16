# TODO: Fix imports in module
import sys
from os.path import dirname

sys.path.append(dirname(__file__))
sys.path.append(dirname(__file__) + "/..")

from unittest import TestCase

from parameterized import parameterized

from ad_writer import LoraCacheSource
from utils import AttrDict


class TestLoraCacheSource(TestCase):
    def setUp(self):
        self.user = self.setup_user()
        self.lc = AttrDict(
            {
                "users": {
                    self.user["uuid"]: [self.user],
                }
            }
        )
        self.lc_historic = AttrDict({})
        self.datasource = LoraCacheSource(self.lc, self.lc_historic)

    def setup_user(self):
        return {
            "uuid": "some_uuid_here",
            "navn": "some_name some_lastname",
            "efternavn": "some_lastname",
            "fornavn": "some_name",
            "kaldenavn": "some_alias some_lastname_alias",
            "kaldenavn_fornavn": "some_alias",
            "kaldenavn_efternavn": "some_lastname_alias",
            "cpr": "some_cpr",
        }

    def test_read_user(self):
        result = self.datasource.read_user("some_uuid_here")
        self.assertEqual(
            result,
            {
                "uuid": "some_uuid_here",
                "name": "some_name some_lastname",
                "surname": "some_lastname",
                "givenname": "some_name",
                "alias": "some_alias some_lastname_alias",
                "alias_givenname": "some_alias",
                "alias_surname": "some_lastname_alias",
                "cpr_no": "some_cpr",
            },
        )
