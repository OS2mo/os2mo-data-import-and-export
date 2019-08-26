

"""
in order to run the os2mo tests Your python-environment must
have all os2mo-requirements and os2mo it self installed.

Furthermore You must define the path to the os2mo source directory

"""

import sys
import os
import pathlib

sys.path[0:0] = [
    os.environ["OS2MO_SRC_DIR"] + "/backend/tests",
    pathlib.Path(__file__).parent / ".."
    ]

import util
import viborg_managers
import requests
from os2mo_helpers.mora_helpers import MoraHelper
import mock
from freezegun import freeze_time

@freeze_time("2019-08-26")
class Tests(util.LoRATestCase):

    def get(self, *args, **kwargs):
        args = list(args)
        if "/service/" in args[0]:
            args[0] = "/service" + args[0].split("/service")[-1]
        response = self.client.open(*args, follow_redirects=True)
        response_mock = mock.MagicMock()
        response_mock.status_code = response.status_code
        response_mock.ok = response.status_code == 200
        response_mock.json = lambda:response.get_json()
        return response_mock

    def setUp(self):
        super().setUp()
        self.mh = MoraHelper()
        requests.get = self.get
        self.load_sql_fixture("normal.sql")

    def test_normal(self):
        self.mh.read_organisation()
        root_uuid = viborg_managers.get_root_org_unit_uuid(self.mh, "Hjørring Kommune")
        nodes = self.mh.read_ou_tree(root_uuid)
        viborg_managers.find_people(self.mh, nodes)
        fieldnames, rows = viborg_managers.prepare_report(self.mh, nodes)
        expected={
            'Leder': 'Martin Fèvre Laustsen',
            'Samlet funktionær': 103,
            'Egen afd': 'Hjørring Kommune',
            'Email': 'martinl@hjorring.dk',
            'Opgjort pr': '26/08/2019',
            'Samlet timeløn': 12,
            'Direkte funktionær': 9,
            'Heraf ledere': 7,
            'Direkte ialt': 9,
            'Direkte timeløn': 0,
            'Samlet ialt': 115
        }
        self.assertEqual(rows[0], expected)



