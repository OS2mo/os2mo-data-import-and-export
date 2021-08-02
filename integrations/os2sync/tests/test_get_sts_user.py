import unittest
from unittest.mock import patch

from parameterized import parameterized

from integrations.os2sync.tests.helpers import MoEmployeeMixin
from integrations.os2sync.tests.helpers import NICKNAME_TEMPLATE


class TestGetStsUser(unittest.TestCase, MoEmployeeMixin):

    maxDiff = None

    def setUp(self):
        super().setUp()
        self._uuid = "mock-uuid"
        self._user_key = "mock-user-key"

    @parameterized.expand(
        [
            # Test without template
            (
                None,  # template config
                dict(nickname=False),  # mo employee response kwargs
                "name",  # key of expected value for `Name`
            ),
            # Test with template: user has no nickname
            (
                {"person.name": NICKNAME_TEMPLATE},  # template config
                dict(nickname=False),  # mo employee response kwargs
                "name",  # key of expected value for `Name`
            ),
            # Test with template: user has a nickname
            (
                {"person.name": NICKNAME_TEMPLATE},  # template config
                dict(nickname=True),  # mo employee response kwargs
                "nickname",  # key of expected value for `Name`
            ),
        ]
    )
    def test_person_template_nickname(
        self,
        os2sync_templates,
        response_kwargs,
        expected_key,
    ):

        mo_employee_response = self.mock_employee_response(**response_kwargs)
        sts_user = self._run(
            mo_employee_response,
            ad_user_key=self._user_key,
            os2sync_templates=os2sync_templates,
        )
        self.assertDictEqual(
            sts_user,
            {
                "Uuid": self._uuid,
                "UserId": self._user_key,
                "Positions": [],
                "Person": {
                    "Name": mo_employee_response.json()[expected_key],
                    "Cpr": mo_employee_response.json()["cpr_no"],
                },
            },
        )

    @parameterized.expand(
        [
            # Test without an AD BVN and without template
            (
                None,  # template config
                None,  # return value of `try_get_ad_user_key`
                "mock-uuid",  # expected value of `UserId` (MO UUID)
            ),
            # Test without an AD BVN, and template which uses `user_key`
            (
                {"person.user_id": "{{ user_key }}"},  # template config
                None,  # return value of `try_get_ad_user_key`
                "testtestesen",  # expected value of `UserId` (MO BVN)
            ),
            # Test without an AD BVN, and template which uses `uuid`
            (
                {"person.user_id": "{{ uuid }}"},  # template config
                None,  # return value of `try_get_ad_user_key`
                "mock-uuid",  # expected value of `UserId` (MO UUID)
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
        mo_employee_response = self.mock_employee_response()
        sts_user = self._run(
            mo_employee_response,
            ad_user_key=given_ad_user_key,
            os2sync_templates=os2sync_templates,
        )
        self.assertDictEqual(
            sts_user,
            {
                "Uuid": self._uuid,
                "UserId": expected_user_id,
                "Positions": [],
                "Person": {
                    "Name": mo_employee_response.json()["name"],
                    "Cpr": mo_employee_response.json()["cpr_no"],
                },
            },
        )

    def _run(self, response, ad_user_key=None, os2sync_templates=None):
        with patch(
            "ra_utils.load_settings.load_settings",
            return_value={
                "os2sync.xfer_cpr": True,
                "os2sync.templates": os2sync_templates or {},
            },
        ):
            from integrations.os2sync.os2mo import get_sts_user as os2mo_get_sts_user

            with self._patch("os2mo_get", response):
                with self._patch("try_get_ad_user_key", ad_user_key):
                    with self._patch("addresses_to_user", []):
                        with self._patch("engagements_to_user", []):
                            return os2mo_get_sts_user(self._uuid, [])

    def _patch(self, name, return_value):
        from integrations.os2sync import os2mo

        return patch.object(os2mo, name, return_value=return_value)
