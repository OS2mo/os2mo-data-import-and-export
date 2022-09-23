from datetime import datetime
from typing import Tuple
from unittest.mock import Mock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

import los_pers
from hypothesis import given
from hypothesis import strategies as st

from .helpers import HelperMixin
from .helpers import mock_config


_azid_it_system_uuid = uuid4()
_mock_config = mock_config(azid_it_system_uuid=_azid_it_system_uuid)
_primary_strategy = st.one_of(st.just("Ja"), st.just("Nej"))


@patch("util.build_cpr_map", return_value={})
class TestPersonImporter:
    @given(st.from_type(los_pers.Person), _primary_strategy)
    def test_generate_employee_az_id_payload_uses_setting(
        self,
        mock_build_cpr_map: Mock,
        person: los_pers.Person,
        primary: str,
    ):
        person.primary = primary
        with _mock_config:
            importer = los_pers.PersonImporter()
        payload = importer.generate_employee_az_id_payload(person, "2021-11-17")
        assert payload["itsystem"]["uuid"] == str(_azid_it_system_uuid)

    @given(st.from_type(los_pers.Person), _primary_strategy)
    def test_ad_accounts_are_not_created(
        self,
        mock_build_cpr_map: Mock,
        person: los_pers.Person,
        primary: str,
    ):
        person.primary = primary
        with _mock_config:
            importer = los_pers.PersonImporter()
        detail_payloads = importer.create_detail_payloads([person], None)

        # Verify that we only generate "AZ" IT users (not "AD")
        it_user_payloads = [
            payload for payload in detail_payloads if payload["type"] == "it"
        ]
        if person.az_id:
            assert len(it_user_payloads) == 1
            assert it_user_payloads[0]["user_key"] == person.az_id
            assert it_user_payloads[0]["itsystem"]["uuid"] == str(_azid_it_system_uuid)
        else:
            assert len(it_user_payloads) == 0


class TestPersonImporterReusesCPR(HelperMixin):
    @given(st.booleans(), st.from_type(los_pers.Person))
    def test_get_person_mo_uuid(self, present: bool, person: los_pers.Person):
        """If the person's CPR is already in MO (present=True), reuse the existing
        person UUID.
        If the person's CPR is not already in MO (present=False), create a new UUID
        derived from the person's CPR.
        """
        # Arrange
        importer, expected_person_uuid = self._get_importer_and_expected_person_uuid(
            present, person
        )
        # Act
        actual_person_uuid = importer._get_person_mo_uuid(person)
        # Assert
        assert actual_person_uuid == expected_person_uuid

    @given(
        st.booleans(), st.from_type(los_pers.Person), _primary_strategy, st.datetimes()
    )
    def test_handle_create_uses_correct_person_uuid(
        self, present: bool, person: los_pers.Person, primary: str, filedate: datetime
    ):
        # Arrange
        person.primary = primary
        importer, expected_person_uuid = self._get_importer_and_expected_person_uuid(
            present, person
        )

        # Act: run `handle_create`
        mock_csv = self._mock_read_csv(person)
        mock_session = self._mock_get_client_session()
        with mock_csv, mock_session:
            with self._mock_create_details() as mock_create_details:
                self._run_until_complete(
                    importer.handle_create("unused_filename.csv", filedate)
                )

        # Assert that we used the proper person UUID in the "create payloads" created
        self._assert_correct_person_uuid_in_create_payloads(
            mock_create_details, expected_person_uuid
        )

    @given(
        st.booleans(), st.from_type(los_pers.Person), _primary_strategy, st.datetimes()
    )
    def test_handle_edit_uses_correct_person_uuid(
        self, present: bool, person: los_pers.Person, primary: str, filedate: datetime
    ):
        # Arrange
        person.primary = primary
        importer, expected_person_uuid = self._get_importer_and_expected_person_uuid(
            present, person
        )

        # Act: run `handle_edit`
        mock_csv = self._mock_read_csv(person)
        mock_session = self._mock_get_client_session()
        with mock_csv, mock_session:
            with self._mock_create_details() as mock_create_details:
                with self._mock_edit_details() as mock_edit_details:
                    # Generate an address UUID in the same manner as
                    # `PersonImporter.generate_employee_email_payload`
                    orgfunk_uuid = importer.uuid_generator(person.cpr + "email")
                    with self._mock_lookup_organisationfunktion(
                        return_value={orgfunk_uuid}
                    ):
                        self._run_until_complete(
                            importer.handle_edit("unused_filename.csv", filedate)
                        )

        # Assert that we used the proper person UUID in the "create payloads" created
        self._assert_correct_person_uuid_in_create_payloads(
            mock_create_details, expected_person_uuid
        )
        # Assert that we used the proper person UUID in the "edit payloads" created
        self._assert_correct_person_uuid_in_edit_payloads(
            mock_edit_details, expected_person_uuid
        )

    def _get_importer_and_expected_person_uuid(
        self, present: bool, person: los_pers.Person
    ) -> Tuple[los_pers.PersonImporter, UUID]:
        expected_person_uuid = uuid4()
        cpr_map = {person.cpr: expected_person_uuid} if present else {}

        with patch("util.build_cpr_map", return_value=cpr_map):
            with _mock_config:
                importer = los_pers.PersonImporter()
                if not present:
                    expected_person_uuid = importer.uuid_generator(person.cpr)

        return importer, expected_person_uuid

    def _assert_correct_person_uuid_in_create_payloads(
        self, mock_create_details: Mock, expected_person_uuid: UUID
    ):
        for payload in mock_create_details.call_args[0][1]:
            if payload["type"] == "employee":
                assert UUID(payload["uuid"]) == expected_person_uuid
            else:
                assert UUID(payload["person"]["uuid"]) == expected_person_uuid

    def _assert_correct_person_uuid_in_edit_payloads(
        self, mock_edit_details: Mock, expected_person_uuid: UUID
    ):
        for payload in mock_edit_details.call_args[0][1]:
            if payload["type"] == "employee":
                assert UUID(payload["data"]["uuid"]) == expected_person_uuid
            else:
                assert UUID(payload["data"]["person"]["uuid"]) == expected_person_uuid
