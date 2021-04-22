import unittest
from unittest.mock import patch

from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from parameterized import parameterized

from exporters.sql_export.sql_table_defs import Bruger
from integrations.os2sync.lcdb_os2mo import get_sts_user as lcdb_get_sts_user
from integrations.os2sync.tests.helpers import NICKNAME_TEMPLATE


# Mock contents of `Bruger` model
_lcdb_mock_users = [
    (
        # When this query occurs:
        [unittest.mock.call.filter(Bruger.uuid == "name only")],
        # Return this object:
        [
            Bruger(
                fornavn="Test",
                efternavn="Testesen",
                kaldenavn_fornavn="",
                kaldenavn_efternavn="",
            ),
        ],
    ),
    (
        # When this query occurs:
        [unittest.mock.call.filter(Bruger.uuid == "name and nickname")],
        # Return this object:
        [
            Bruger(
                fornavn="Test",
                efternavn="Testesen",
                kaldenavn_fornavn="Teste",
                kaldenavn_efternavn="Testersen",
            ),
        ],
    ),
]


class TestGetStsUser(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._session = UnifiedAlchemyMagicMock(data=_lcdb_mock_users)

    @parameterized.expand(
        [
            # Test without template
            (
                None,  # template
                "name only",  # uuid of mock `Bruger`
                "Test Testesen",  # expected value of `Name`
            ),
            # Test with template: user has no nickname
            (
                NICKNAME_TEMPLATE,  # template
                "name only",  # uuid of mock `Bruger`
                "Test Testesen",  # expected value of `Name`
            ),
            # Test with template: user has no nickname
            (
                NICKNAME_TEMPLATE,  # template
                "name and nickname",  # uuid of mock `Bruger`
                "Teste Testersen",  # expected value of `Name`
            ),
        ]
    )
    def test_get_sts_user(self, template, uuid, expected_name):
        if template:
            # Run with template
            with patch.dict("integrations.os2sync.config.settings") as settings:
                settings["OS2SYNC_TEMPLATES"]["person.name"] = template
                sts_user = lcdb_get_sts_user(self._session, uuid, [])
        else:
            # Run without template
            sts_user = lcdb_get_sts_user(self._session, uuid, [])

        self.assertDictEqual(
            sts_user,
            {
                "Uuid": uuid,
                "UserId": uuid,
                "Positions": [],
                "Person": {
                    "Name": expected_name,
                    "Cpr": None,
                },
            },
        )
