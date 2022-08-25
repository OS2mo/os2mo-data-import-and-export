from uuid import uuid4

import los_pers
from hypothesis import given
from hypothesis import strategies as st

from .helpers import mock_config


class TestPersonImporter:
    _azid_it_system_uuid = uuid4()

    @given(st.builds(los_pers.Person))
    def test_generate_employee_az_id_payload_uses_setting(self, person):
        with mock_config(azid_it_system_uuid=self._azid_it_system_uuid):
            importer = los_pers.PersonImporter()
        payload = importer.generate_employee_az_id_payload(person, "2021-11-17")
        assert payload["itsystem"]["uuid"] == str(self._azid_it_system_uuid)

    @given(
        st.builds(los_pers.Person),
        st.one_of(st.just("Ja"), st.just("Nej")),
    )
    def test_ad_accounts_are_not_created(self, person, primary):
        person.primary = primary
        with mock_config(azid_it_system_uuid=self._azid_it_system_uuid):
            importer = los_pers.PersonImporter()
        detail_payloads = importer.create_detail_payloads([person], None)
        it_user_payloads = [
            payload for payload in detail_payloads if payload["type"] == "it"
        ]
        if person.az_id:
            assert len(it_user_payloads) == 1
            assert it_user_payloads[0]["user_key"] == person.az_id
            assert it_user_payloads[0]["itsystem"]["uuid"] == str(
                self._azid_it_system_uuid
            )
        else:
            assert len(it_user_payloads) == 0
