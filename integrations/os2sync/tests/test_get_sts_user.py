import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from integrations.os2sync import os2mo
from integrations.os2sync.os2mo import get_sts_user as os2mo_get_sts_user
from integrations.os2sync.tests.test_person_conversions import _EmployeeMixin


class TestGetStsUser(unittest.TestCase, _EmployeeMixin):
    maxDiff = None

    def test_name_only(self):
        pass

    def test_name_and_nickname(self):
        template = "{% if nickname -%}{{ nickname }}{%- else %}{{ name }}{%- endif %}"
        with patch.dict("integrations.os2sync.config.settings") as settings:
            settings["OS2SYNC_XFER_CPR"] = True
            settings["OS2SYNC_TEMPLATES"]["person.name"] = template
            response = self._make_mock_response()
            with self._fn("os2mo_get", response):
                with self._fn("try_get_ad_user_key", MagicMock()):
                    with self._fn("addresses_to_user", []):
                        with self._fn("engagements_to_user", []):
                            sts_user = os2mo_get_sts_user("name and nickname", [])

        self.assertDictEqual(
            sts_user,
            {
                "Uuid": "name and nickname",
                "UserId": MagicMock(),
                "Positions": [],
                "Person": {
                    "Name": "Test Testesen",
                    "Cpr": None,
                },
            },
        )

    def _fn(self, name, return_value):
        return patch.object(os2mo, name, return_value=return_value)

    def _make_mock_response(self, **kwargs):
        mo_employee = self.mock_employee(**kwargs)

        class MockResponse:
            def json(self):
                return mo_employee

        return MockResponse()