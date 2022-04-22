import unittest
from unittest.mock import patch

from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from helpers import dummy_settings
from parameterized import parameterized

from exporters.sql_export.sql_table_defs import Bruger
from integrations.os2sync import lcdb_os2mo
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
                bvn="mo-user-key",
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
                bvn="mo-user-key",
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
    @patch("integrations.os2sync.os2mo.org_unit_uuids", return_value={})
    def test_person_template_nickname(
        self, template, uuid, expected_name, allowed_unitids_mock
    ):
        settings = dummy_settings
        if template:
            # Run with template
            settings.os2sync_templates["person.name"] = template
            sts_user = lcdb_os2mo.get_sts_user(self._session, uuid, settings=settings)
        else:
            # Run without template
            sts_user = lcdb_os2mo.get_sts_user(self._session, uuid, settings=settings)

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
    @patch("integrations.os2sync.os2mo.org_unit_uuids", return_value={})
    def test_user_template_user_id(
        self,
        os2sync_templates,
        given_ad_user_key,
        expected_user_id,
        allowed_unitids_mock,
    ):
        mo_user_uuid = "name only"
        settings = dummy_settings
        settings.os2sync_templates = os2sync_templates or {}
        with self._patch("try_get_ad_user_key", given_ad_user_key):
            sts_user = lcdb_os2mo.get_sts_user(
                self._session, mo_user_uuid, settings=dummy_settings
            )

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

    def _patch(self, name, return_value):
        return patch.object(lcdb_os2mo, name, return_value=return_value)
