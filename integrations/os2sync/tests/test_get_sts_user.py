import unittest
from unittest.mock import patch

from parameterized import parameterized

from integrations.os2sync import os2mo
from integrations.os2sync.os2mo import get_sts_user as os2mo_get_sts_user
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
                None,  # template
                dict(nickname=False),  # mo employee response kwargs
                "name",  # key of expected value for `Name`
            ),
            # Test with template: user has no nickname
            (
                NICKNAME_TEMPLATE,  # template
                dict(nickname=False),  # mo employee response kwargs
                "name",  # key of expected value for `Name`
            ),
            # Test with template: user has a nickname
            (
                NICKNAME_TEMPLATE,  # template
                dict(nickname=True),  # mo employee response kwargs
                "nickname",  # key of expected value for `Name`
            ),
        ]
    )
    def test_get_sts_user(self, template, response_kwargs, expected_key):
        mo_employee_response = self.mock_employee_response(**response_kwargs)
        sts_user = self._run(mo_employee_response, template=template)
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

    def _run(self, response, template=None):
        with patch.dict("integrations.os2sync.config.settings") as settings:
            settings["OS2SYNC_XFER_CPR"] = True
            if template:
                settings["OS2SYNC_TEMPLATES"]["person.name"] = template
            with self._patch("os2mo_get", response):
                with self._patch("try_get_ad_user_key", self._user_key):
                    with self._patch("addresses_to_user", []):
                        with self._patch("engagements_to_user", []):
                            return os2mo_get_sts_user(self._uuid, [])

    def _patch(self, name, return_value):
        return patch.object(os2mo, name, return_value=return_value)
