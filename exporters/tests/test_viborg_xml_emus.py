#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
in order to run the os2mo tests Your python-environment must
have all os2mo-requirements and os2mo it self installed.

Furthermore You must define the path to the os2mo source directory

in order to record a test result use this temporarily in a test,
adapted to Your test: self.cache_new_result("file contents")
"""

import sys
import os
import pathlib
import requests
from os2mo_helpers.mora_helpers import MoraHelper
import mock
from freezegun import freeze_time
import io

# emus report setup - import time dependencies
os.environ["MORA_ROOT_ORG_UNIT_NAME"] = "Hjørring Kommune"
os.environ["EMUS_RESPONSIBILITY_CLASS"] = "07b8b1f5-a441-46d4-b523-c2f44a6dd538"

from viborg_xml_emus import main as generate_file  # noqa


sys.path[0:0] = [
    os.environ["OS2MO_SRC_DIR"] + "/backend/tests",
    pathlib.Path(__file__).parent / ".."
    ]
import util  # noqa


testdata = pathlib.Path(__file__).resolve().parent / "data"


@freeze_time("2019-08-26")
class Tests(util.LoRATestCase):
    maxDiff = None

    def cache_new_result(self, generated_file):
        with open(self._test_data_result, "w", encoding="utf-8") as f:
            f.write(generated_file.getvalue())

    def read_old_result(self):
        with open(self._test_data_result, "r", encoding="utf-8") as f:
            return f.read()

    def get(self, *args, **kwargs):
        args = list(args)
        # recognize mo?
        if "/service/" in args[0]:
            args[0] = "/service" + args[0].split("/service")[-1]
            response = self.client.open(*args, follow_redirects=True)
            response_mock = mock.MagicMock()
            response_mock.status_code = response.status_code
            response_mock.ok = response.status_code == 200
            response_mock.json = lambda: response.get_json()
            return response_mock
        else:
            return requests._orgget(*args, **kwargs)

    def setUp(self):
        super().setUp()
        util.amqp.publish_message = lambda a, b, c, d, e: None
        self.mh = MoraHelper()
        if not getattr(requests, "_orgget", False):
            requests._orgget = requests.get
            requests.get = self.get
        self._test_data_result = str(
            testdata / (
                pathlib.Path(__file__).stem +
                "_" +
                self._testMethodName +
                "_result.xml"
            )
        )

    def tearDown(self):
        if getattr(requests, "_orgget", False):
            requests.get = requests._orgget
            del requests._orgget

    def run_the_program(self):
        generated_file = io.StringIO()
        generate_file(
            emus_xml_file=generated_file,
            mh=self.mh
        )
        return generated_file

    def test_with_new_mikkel(self):
        "initially approved report run on the normal.sql fixture from os2mo tests"
        self.load_sql_fixture("normal.sql")

        # mikke must be in the report
        self.assertRequest("service/e/create", json={
            "name": "Mikkel Petersen",
            "cpr_no": "1001031333",
            "org": {"uuid": "c5395419-4c76-417f-9939-5a4bf81648d8"},
            "details": [{
                "type": "engagement",
                "primary": True,
                "org_unit": {"uuid": "23a2ace2-52ca-458d-bead-d1a42080579f"},
                "job_function": {"uuid": "45c19d33-b65c-4e1e-a890-51bd3bf26e2b"},
                "engagement_type": {"uuid": "60315fce-995c-4874-ad7b-48b27aaafb25"},
                "validity": {"from": "2019-08-01", "to": None}
            }],
            "force": True
        })
        generated_file = self.run_the_program()
        self.assertIn('<cpr>1001031333</cpr>', generated_file.getvalue())

    def test_without_future_mikkel(self):
        self.load_sql_fixture("normal.sql")

        # mikkel must not be in the report
        self.assertRequest("service/e/create", json={
            "name": "Mikkel Petersen",
            "cpr_no": "1001031333",
            "org": {"uuid": "c5395419-4c76-417f-9939-5a4bf81648d8"},
            "details": [{
                "type": "engagement",
                "primary": True,
                "org_unit": {"uuid": "23a2ace2-52ca-458d-bead-d1a42080579f"},
                "job_function": {"uuid": "45c19d33-b65c-4e1e-a890-51bd3bf26e2b"},
                "engagement_type": {"uuid": "60315fce-995c-4874-ad7b-48b27aaafb25"},
                "validity": {"from": "2019-10-01", "to": None}
            }],
            "force": True
        })
        generated_file = self.run_the_program()
        self.assertNotIn('<cpr>1001031333</cpr>', generated_file.getvalue())
