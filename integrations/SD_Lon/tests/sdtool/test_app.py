# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import datetime
from unittest import TestCase
from unittest.mock import patch
from uuid import UUID

from fastapi.testclient import TestClient

from tests.sdtool.utils import EnvironmentVarGuard


class AppTests(TestCase):
    def setUp(self):
        self.env = EnvironmentVarGuard()
        self.env.set("MORA_URL", "http://example.org")
        self.env.set("SD_USER", "sd_user")
        self.env.set("SD_PASSWORD", "sd_password")
        self.env.set("SD_INSTITUTION_IDENTIFIER", "sd_institution_identifier")

        with self.env:
            from sdtool.main import app

            self.app = app
            self.client = TestClient(app)

    def test_triggers(self):
        response = self.client.get("/triggers")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        trigger = payload[0]
        self.assertEqual(trigger["url"], "/triggers/ou/refresh")
        self.assertEqual(trigger["role_type"], "org_unit")

    @patch("sdtool.main.datetime")
    @patch("sdtool.main.unit_fixer")
    def test_ou_edit(self, unit_fixer, mock_datatime):
        mock_datatime.datetime.now.return_value = datetime.datetime(
            2000, 1, 1, 12, 13
        )
        expected = {"msg": f"SD-Tool opdatering påbegyndt 12:13. Genindlæs siden om nogle minutter."}
        unit_fixer.return_value = expected

        uuid = "fb2d158f-114e-5f67-8365-2c520cf10b58"
        response = self.client.post(
            "/triggers/ou/refresh",
            json={
                "event_type": "ON_BEFORE",
                "request": {"uuid": uuid},
                "request_type": "REFRESH",
                "role_type": "org_unit",
                "uuid": uuid,
            },
        )
        unit_fixer.assert_called_with(UUID(uuid))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload, expected)
