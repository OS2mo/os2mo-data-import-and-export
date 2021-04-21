import unittest

from alchemy_mock.mocking import UnifiedAlchemyMagicMock

from exporters.sql_export.sql_table_defs import Bruger
from integrations.os2sync.lcdb_os2mo import get_sts_user as lcdb_get_sts_user


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

    def test_name_only(self):
        sts_user = lcdb_get_sts_user(self._session, "name only", [])

        self.assertDictEqual(
            sts_user,
            {
                "Uuid": "name only",
                "UserId": "name only",
                "Positions": [],
                "Person": {
                    "Name": "Test Testesen",
                    "Cpr": None,
                },
            },
        )

    def test_name_and_nickname(self):
        template = "{% if nickname -%}{{ nickname }}{%- else %}{{ name }}{%- endif %}"
        dict_path = "integrations.os2sync.config.settings"
        with unittest.mock.patch.dict(dict_path) as patched_settings:
            patched_settings["OS2SYNC_TEMPLATES"]["person.name"] = template
            sts_user = lcdb_get_sts_user(self._session, "name and nickname", [])

        self.assertDictEqual(
            sts_user,
            {
                "Uuid": "name and nickname",
                "UserId": "name and nickname",
                "Positions": [],
                "Person": {
                    "Name": "Teste Testersen",
                    "Cpr": None,
                },
            },
        )
