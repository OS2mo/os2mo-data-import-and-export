from uuid import uuid4

import los_pers
from hypothesis import given
from hypothesis import strategies as st

from .helpers import mock_config


class TestPersonImporter:
    _azid_it_system_uuid = uuid4()

    @given(st.builds(los_pers.Person))
    def test_generate_employee_az_id_payload_uses_setting(self, person):
        importer = self._get_configured_importer()
        payload = importer.generate_employee_az_id_payload(person, "2021-11-17")
        assert payload["itsystem"]["uuid"] == str(self._azid_it_system_uuid)

    def _get_configured_importer(self):
        with mock_config(azid_it_system_uuid=self._azid_it_system_uuid):
            return los_pers.PersonImporter()
