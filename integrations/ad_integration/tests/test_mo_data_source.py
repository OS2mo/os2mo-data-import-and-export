from unittest import TestCase

from ..ad_writer import LoraCacheSource
from ..utils import AttrDict


class TestLoraCacheSource(TestCase):
    def setUp(self):
        self.user = self.setup_user()
        self.lc = AttrDict(
            {
                "users": {
                    self.user["uuid"]: [self.user],
                },
                "engagements": {
                    "key-1": [
                        # Current registration
                        {
                            "user": self.user["uuid"],
                            "from_date": "2020-01-01",
                            "to_date": None,
                        },
                        # Previous registration
                        {
                            "user": self.user["uuid"],
                            "from_date": "2019-01-01",
                            "to_date": "2021-01-01",
                        },
                    ],
                },
            }
        )
        self.lc_historic = AttrDict(
            {
                "engagements": {
                    # Add some historic engagements that must not be read by
                    # `get_engagement_dates`.
                    "key-1": [
                        # Current historic registration
                        {
                            "user": self.user["uuid"],
                            "from_date": "2010-01-01",
                            "to_date": "2019-12-31",
                        },
                    ]
                }
            }
        )
        self.datasource = LoraCacheSource(self.lc, self.lc_historic, None)

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
                "nickname": "some_alias some_lastname_alias",
                "nickname_givenname": "some_alias",
                "nickname_surname": "some_lastname_alias",
                "cpr_no": "some_cpr",
            },
        )

    def test_get_engagement_dates(self):
        result = self.datasource.get_engagement_dates(self.user["uuid"])
        self.assertEqual(
            [list(elem) for elem in result],  # consume each iterable in result
            [["2020-01-01"], [None]],
        )

    def test_get_engagement_endpoint_dates(self):
        result = self.datasource.get_engagement_endpoint_dates(self.user["uuid"])
        # "to_date" of None must be converted into "9999-12-31"
        self.assertEqual(result, ("2020-01-01", "9999-12-31"))
