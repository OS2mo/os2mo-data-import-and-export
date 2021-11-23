import unittest
from unittest.mock import patch

from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from parameterized import parameterized

from exporters.sql_export.sql_table_defs import Bruger
from exporters.sql_export.sql_table_defs import Engagement
from integrations.os2sync import lcdb_os2mo
from integrations.os2sync.tests.helpers import NICKNAME_TEMPLATE


# Mock contents of `Bruger` and `Engagement` database models
def _lcdb_mock_user(
    bruger_uuid, fornavn, efternavn, kaldenavn_fornavn="", kaldenavn_efternavn=""
):
    return (
        # When this query occurs:
        [unittest.mock.call.filter(Bruger.uuid == bruger_uuid)],
        # Return this object:
        [
            Bruger(
                fornavn=fornavn,
                efternavn=efternavn,
                kaldenavn_fornavn=kaldenavn_fornavn,
                kaldenavn_efternavn=kaldenavn_efternavn,
                bvn="mo-user-key",
            ),
        ],
    )


def _lcdb_mock_engagement(bruger_uuid):
    return (
        # When this query occurs:
        [unittest.mock.call.filter(Engagement.bruger_uuid == bruger_uuid)],
        # Return this object:
        [
            Engagement(
                uuid="engagements-uuid",
                bruger_uuid=bruger_uuid,
                enhed_uuid="enheds-uuid",
                primær_boolean=True,
                stillingsbetegnelse_titel="stillingsbetegnelse-titel",
            ),
        ],
    )


_lcdb_mock_database = [
    _lcdb_mock_user("name only", "Test", "Testesen"),
    _lcdb_mock_user("name and nickname", "Test", "Testesen", "Teste", "Testersen"),
    # For "test_reads_is_primary"
    _lcdb_mock_user("bruger-uuid", "Bla", "Blabla"),
    _lcdb_mock_engagement("bruger-uuid"),
]


class TestGetStsUser(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._session = UnifiedAlchemyMagicMock(data=_lcdb_mock_database)

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
    def test_person_template_nickname(self, template, uuid, expected_name):
        if template:
            # Run with template
            with patch.dict("integrations.os2sync.config.settings") as settings:
                settings["OS2SYNC_TEMPLATES"]["person.name"] = template
                sts_user = lcdb_os2mo.get_sts_user(self._session, uuid, [])
        else:
            # Run without template
            sts_user = lcdb_os2mo.get_sts_user(self._session, uuid, [])

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

    @parameterized.expand(
        [
            # Test without an AD BVN and without template
            (
                None,  # template config
                None,  # return value of `try_get_ad_user_key`
                "name only",  # expected value of `UserId` (MO UUID)
            ),
            # Test without an AD BVN, and template which uses `user_key`
            (
                {"person.user_id": "{{ user_key }}"},  # template config
                None,  # return value of `try_get_ad_user_key`
                "mo-user-key",  # expected value of `UserId` (MO BVN)
            ),
            # Test without an AD BVN, and template which uses `uuid`
            (
                {"person.user_id": "{{ uuid }}"},  # template config
                None,  # return value of `try_get_ad_user_key`
                "name only",  # expected value of `UserId` (MO UUID)
            ),
            # Test with an AD BVN, but without template
            (
                None,  # template config
                "mock-ad-bvn",  # return value of `try_get_ad_user_key`
                "mock-ad-bvn",  # expected value of `UserId` (AD BVN)
            ),
            # Test with an AD BVN, and template which uses `user_key`
            (
                {"person.user_id": "{{ user_key }}"},  # template config
                "mock-ad-bvn",  # return value of `try_get_ad_user_key`
                "mock-ad-bvn",  # expected value of `UserId` (AD BVN)
            ),
            # Test with an AD BVN, and template which uses `uuid`
            (
                {"person.user_id": "{{ uuid }}"},  # template config
                "mock-ad-bvn",  # return value of `try_get_ad_user_key`
                "mock-ad-bvn",  # expected value of `UserId` (AD BVN)
            ),
        ]
    )
    def test_user_template_user_id(
        self,
        os2sync_templates,
        given_ad_user_key,
        expected_user_id,
    ):
        mo_user_uuid = "name only"
        with patch.dict("integrations.os2sync.config.settings") as settings:
            settings["OS2SYNC_TEMPLATES"] = os2sync_templates or {}
            with self._patch("try_get_ad_user_key", given_ad_user_key):
                sts_user = lcdb_os2mo.get_sts_user(self._session, mo_user_uuid, [])

        self.assertDictEqual(
            sts_user,
            {
                "Uuid": mo_user_uuid,
                "UserId": expected_user_id,
                "Positions": [],
                "Person": {
                    "Name": "Test Testesen",
                    "Cpr": None,
                },
            },
        )

    def test_reads_is_primary(self):
        sts_user = lcdb_os2mo.get_sts_user(
            self._session, "bruger-uuid", ["enheds-uuid"]
        )
        self.assertListEqual(
            sts_user["Positions"],
            [
                {
                    "OrgUnitUuid": "enheds-uuid",
                    "Name": "stillingsbetegnelse-titel",
                    "is_primary": True,
                }
            ],
        )

    def _patch(self, name, return_value):
        return patch.object(lcdb_os2mo, name, return_value=return_value)
