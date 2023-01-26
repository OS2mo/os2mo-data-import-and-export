import unittest
from datetime import datetime
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

from hypothesis import given
from hypothesis import settings
from hypothesis.strategies import datetimes
from hypothesis.strategies import text
from hypothesis.strategies import uuids
from parameterized import parameterized

from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport


class OpusDiffImportTestbase(OpusDiffImport):
    @patch("integrations.calculate_primary.opus.OPUSPrimaryEngagementUpdater")
    @patch(
        "integrations.calculate_primary.opus.OPUSPrimaryEngagementUpdater.__init__",
        return_value=None,
    )
    def __init__(self, latest_date, primary_init_mock, mock_primary, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"
        self.morahelper_mock._mo_post.return_value.status_code = 201
        self.morahelper_mock.ensure_class_in_facet.return_code = uuid4()
        mock_primary.primary_types = {"non_primary": "test"}

        super().__init__(latest_date, *args, **kwargs)

    def _get_mora_helper(self, hostname, use_cache):
        return self.morahelper_mock

    def _find_classes(self, facet):
        if facet == "engagement_type":
            return ({"Ansat": "eng_type", "Softwaretester": "dummy"}, facet)
        if facet == "org_unit_type":
            return ({"uuid": "eng_type"}, facet)
        if facet == "org_unit_address_type":
            return (
                {
                    "SE": "SE_UUID",
                    "CVR": "CVR_UUID",
                    "EAN": "EAN_UUID",
                    "Pnummer": "PN_UUID",
                    "PhoneUnit": "Phone_UUID",
                    "AddressPostUnit": "Addr_UUID",
                },
                facet,
            )
        if facet == "employee_address_type":
            return (
                {
                    "AdressePostEmployee": "post_UUID",
                    "EmailEmployee": "email_UUID",
                    "PhoneEmployee": "phone_UUID",
                },
                facet,
            )
        return ({"uuid": "eng_type"}, facet)

    def _add_klasse_to_lora(self, klasse_name, facet_uuid):
        return self._add_klasse_to_lora


class OpusDiffImportTest_updates(OpusDiffImportTestbase):
    def __init__(self, *args, **kwargs):
        self._update_unit_types = MagicMock()
        self._update_unit_types.return_value = "DummyUUID"
        self.morahelper_mock._mo_post.return_value.status_code = 200
        self._perform_address_update = MagicMock()
        super().__init__(*args, **kwargs)

    def _update_unit_types(self, org_type):
        return self._update_unit_types

    def _perform_address_update(self, args, current, address_dict):
        return self._perform_address_update


class OpusDiffImportTest_counts(OpusDiffImportTestbase):
    def __init__(self, *args, **kwargs):
        self.update_employee = MagicMock()
        self.update_unit = MagicMock()
        self.terminate_detail = MagicMock()
        self._find_engagement = MagicMock()

        super().__init__(*args, **kwargs)

    def update_employee(self, employee):
        return self.update_employee

    def update_unit(self, unit):
        return self.update_unit

    def terminate_detail(self, uuid, detail_type="engagement", end_date=None):
        return self.terminate_detail

    def _find_engagement(self, bvn, present=False):
        return self._find_engagement


class Opus_diff_import_tester(unittest.TestCase):
    """Tests for opus_diff_import funktions that does not use input from testfiles"""

    def setUp(self):
        self.file1 = Path.cwd() / "integrations/opus/tests/ZLPE20200101_delta.xml"
        self.file2 = Path.cwd() / "integrations/opus/tests/ZLPE20200102_delta.xml"
        self.expected_unit_count = 4
        self.expected_employee_count = 3
        self.expected_terminations = 1

        filter_ids = []
        (
            units,
            filtered_units,
            employees,
            terminated_employees,
        ) = opus_helpers.read_and_transform_data(self.file1, self.file2, filter_ids)
        self.units = list(units)
        self.filtered_units = list(filtered_units)
        self.employees = list(employees)
        self.terminated_employees = list(terminated_employees)

    def test_file_diff(self):
        self.assertEqual(len(self.units), self.expected_unit_count)
        self.assertEqual(
            len(self.employees),
            self.expected_employee_count,
        )

    @given(datetimes())
    def test_import_unit_count(self, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTest_counts(
            xml_date, ad_reader=None, employee_mapping="test"
        )
        diff.start_import(self.units, self.employees, self.terminated_employees)
        self.assertEqual(diff.update_unit.call_count, self.expected_unit_count)

    @given(datetimes())
    def test_import_employee_count(self, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTest_counts(
            xml_date, ad_reader=None, employee_mapping="test"
        )
        diff.start_import(self.units, self.employees, self.terminated_employees)
        self.assertEqual(diff.update_employee.call_count, self.expected_employee_count)

    @given(datetimes())
    def test_termination(self, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTest_counts(
            xml_date, ad_reader=None, employee_mapping="test"
        )
        diff.start_import(self.units, self.employees, self.terminated_employees)
        self.assertEqual(
            diff._find_engagement.call_count, self.expected_terminations * 2
        )

    @patch("integrations.dawa_helper.dawa_lookup")
    @settings(deadline=None)
    @given(datetimes())
    def test_update_unit(self, dawa_helper_mock, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None, employee_mapping="test")
        diff.ensure_class_in_facet = MagicMock(return_value="dummy-class-uuid")
        for unit in self.units:
            diff.update_unit(unit)
            calculated_uuid = opus_helpers.generate_uuid(unit["@id"])
            if unit.get("street"):
                diff.helper._mo_post.assert_called_with(
                    "details/create",
                    {
                        "type": "address",
                        "value": dawa_helper_mock(),
                        "address_type": {"uuid": "dummy-class-uuid"},
                        "validity": {"from": xml_date.strftime("%Y-%m-%d"), "to": None},
                        "org_unit": {"uuid": str(calculated_uuid)},
                        "visibility": None,
                    },
                )

    @patch("integrations.dawa_helper.dawa_lookup")
    @settings(deadline=None)
    @given(datetimes())
    def test_update_employee(self, dawa_helper_mock, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None, employee_mapping="test")
        diff.it_systems = {"Opus": "Opus_uuid"}
        diff.updater.primary_types = {"non_primary": "test"}
        diff.updater.set_current_person = MagicMock()
        diff.updater.recalculate_primary = MagicMock()
        diff.ensure_class_in_facet = MagicMock()
        with patch(
            "integrations.opus.opus_diff_import.OpusDiffImport._assert",
            return_value=None,
        ):
            for employee in self.employees:
                if employee.get("cpr"):
                    diff.update_employee(employee)
                    uuid = diff.helper._mo_lookup().__getitem__().__getitem__()
                    diff.helper._mo_post.assert_called_with(
                        "details/terminate",
                        {
                            "type": "manager",
                            "uuid": uuid,
                            "validity": {"to": xml_date.strftime("%Y-%m-%d")},
                        },
                    )
                else:
                    self.assertEqual(employee["@action"], "leave")

    @given(datetimes(), datetimes(), text(), uuids(), uuids(), uuids())
    def test_perform_address_update_create(
        self, xml_date, fromdate, value, address_type_uuid, org_unit_uuid, visibility
    ):
        diff = OpusDiffImportTestbase(
            xml_date, ad_reader=None, employee_mapping={"dummy": 1}
        )
        address_type_uuid = str(address_type_uuid)
        args = {
            "address_type": {"uuid": address_type_uuid},
            "value": value,
            "validity": {"from": fromdate.strftime("%Y-%m-%d"), "to": None},
            "unit_uuid": str(org_unit_uuid),
            "visibility": {"uuid": str(visibility)},
        }
        current = {"uuid": address_type_uuid}
        # Same
        diff._perform_address_update(args, current)
        diff.helper.assert_not_called()
        # new
        diff._perform_address_update(args, None)
        diff.helper._mo_post.assert_called_with(
            "details/create",
            {
                "type": "address",
                "value": value,
                "address_type": {"uuid": address_type_uuid},
                "validity": {"from": fromdate.strftime("%Y-%m-%d"), "to": None},
                "org_unit": {"uuid": str(org_unit_uuid)},
                "visibility": {"uuid": str(visibility)},
            },
        )

    @parameterized.expand(
        [
            (None, None, None),
            (None, "Username", "details/create"),
            ("Username", "new_username", "details/edit"),
            ("new_username", None, "details/terminate"),
        ]
    )
    @patch("integrations.dawa_helper.dawa_lookup")
    @settings(deadline=None)
    @given(datetimes())
    def test_update_username(
        self,
        current_username,
        new_username,
        change_type,
        dawa_helper_mock,
        xml_date,
    ):
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None, employee_mapping="test")
        date = xml_date.strftime("%Y-%m-%d")
        diff.it_systems = {"Opus": "Opus_uuid"}
        diff.morahelper_mock.get_e_itsystems.return_value = [
            {"user_key": current_username, "uuid": "dummyuuid"}
            if current_username
            else None
        ]

        if change_type == "details/edit":
            expected = {
                "type": "it",
                "uuid": "dummyuuid",
                "data": {
                    "user_key": new_username,
                    "validity": {"from": date, "to": None},
                },
            }
        elif change_type == "details/create":
            expected = {
                "type": "it",
                "user_key": new_username,
                "itsystem": {"uuid": "Opus_uuid"},
                "person": {"uuid": "dummyuuid"},
                "validity": {"from": date, "to": None},
            }
        elif change_type == "details/terminate":
            expected = {
                "type": "it",
                "uuid": "dummyuuid",
                "validity": {"to": date},
            }
            diff.morahelper_mock._mo_post.return_value.status_code = 200

        diff.connect_it_system(new_username, "Opus", {}, "dummyuuid")
        if change_type:
            diff.morahelper_mock._mo_post.assert_called_once_with(change_type, expected)
        else:
            diff.morahelper_mock._mo_post.assert_not_called()

    @patch("integrations.dawa_helper.dawa_lookup")
    @given(datetimes())
    def test_skip_multiple_usernames(
        self,
        dawa_helper_mock,
        xml_date,
    ):
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None, employee_mapping="test")
        diff.it_systems = {"Opus": "Opus_uuid"}
        diff.morahelper_mock.get_e_itsystems.return_value = [
            {"user_key": "username1", "uuid": "dummyuuid"},
            {"user_key": "username2", "uuid": "dummyuuid"},
        ]
        with self.assertLogs() as cm:
            diff.connect_it_system("new_username", "Opus", {}, "personuuid")
            diff.morahelper_mock._mo_post.assert_not_called()
            assert cm.output == [
                "WARNING:opusImport:Skipped connecting Opus IT system . More than one "
                "IT system found for person_uuid='personuuid'"
            ]

    @patch("integrations.opus.opus_helpers.find_opus_root_unit_uuid")
    def test_ensure_class_in_facet(self, root_uuid_mock):
        """Tests that calling ensure_class_in_facet calls morahelpers with the correct owner added"""
        root_uuid = uuid4()
        root_uuid_mock.return_value = root_uuid
        diff = OpusDiffImportTestbase(
            "2022-07-13", ad_reader=None, employee_mapping="test"
        )

        diff.ensure_class_in_facet("Facetname", "classbvn")
        diff.helper.ensure_class_in_facet.assert_called_once_with(
            "Facetname", "classbvn", owner=root_uuid
        )


class _GetInstanceMixin:
    _xml_date = datetime.now()

    def get_instance(self, settings: dict) -> OpusDiffImport:
        settings.setdefault("mora.base", "http://unused.url")
        with patch(
            "integrations.opus.opus_diff_import.load_settings", return_value=settings
        ):
            with patch("integrations.opus.opus_diff_import.MoraHelper"):
                with patch(
                    "integrations.opus.opus_diff_import.OPUSPrimaryEngagementUpdater"
                ):
                    instance = OpusDiffImport(
                        xml_date=self._xml_date,
                        ad_reader=None,
                        employee_mapping=object(),
                    )
                    return instance


class TestCondenseEmployeeOpusAddresses(_GetInstanceMixin):
    """Test `OpusDiffImporter._condense_employee_opus_addresses`"""

    opus_employee = {
        "email": "foobar@example.com",
        "workPhone": "12345678",
        "address": "Testvej 1",
        "postalCode": "1234",
    }

    opus_employee_protected_address = {
        "email": "foobar@example.com",
        "workPhone": "12345678",
        # non-empty dict means the address is protected (?)
        "address": {"unknown": "content"},
        "postalCode": "1234",
    }

    dar_valid_uuid = "dar-valid-address-uuid"
    dar_invalid_uuid = None

    @parameterized.expand(
        [
            # Test flow with all feature flags turned off
            (
                {},  # empty feature flags
                opus_employee,
                dar_valid_uuid,
                {
                    "phone": opus_employee["workPhone"],
                    "email": opus_employee["email"],
                    "dar": dar_valid_uuid,
                },
            ),
            # Test "skip_employee_email" flag with normal Opus employee and valid DAR
            (
                {"integrations.opus.skip_employee_email": True},
                opus_employee,
                dar_valid_uuid,
                {"phone": opus_employee["workPhone"], "dar": dar_valid_uuid},
            ),
            # Test "skip_employee_address" flag with normal Opus employee and valid DAR
            (
                {"integrations.opus.skip_employee_address": True},
                opus_employee,
                dar_valid_uuid,
                {"phone": opus_employee["workPhone"], "email": opus_employee["email"]},
            ),
            # Test "skip_employee_phone" flag with normal Opus employee and valid DAR
            (
                {"integrations.opus.skip_employee_phone": True},
                opus_employee,
                dar_valid_uuid,
                {"email": opus_employee["email"], "dar": dar_valid_uuid},
            ),
            # Test protected addresses are always removed (regardless of
            # "skip_employee_address" flag.)
            (
                {"integrations.opus.skip_employee_address": False},
                opus_employee_protected_address,
                dar_valid_uuid,
                {"email": opus_employee["email"], "phone": opus_employee["workPhone"]},
            ),
            # Test that failed DAR lookups result in no "dar" key being added to the
            # result (regardless of "skip_employee_address" flag.)
            (
                {"integrations.opus.skip_employee_address": False},
                opus_employee,
                dar_invalid_uuid,
                {"email": opus_employee["email"], "phone": opus_employee["workPhone"]},
            ),
        ]
    )
    def test_feature_flags_are_respected(
        self,
        settings: dict,
        opus_employee: dict,
        dar_response: Optional[str],
        expected_result: dict,
    ) -> None:
        instance = self.get_instance(settings)
        with patch(
            "integrations.opus.opus_diff_import.dawa_helper.dawa_lookup",
            return_value=dar_response,
        ):
            actual_result = instance._condense_employee_opus_addresses(opus_employee)
            assert actual_result == expected_result


class TestUpdateEmployeeAddress(_GetInstanceMixin):
    """Test `OpusDiffImporter._update_employee_address`"""

    opus_employee = {
        "email": "foobar@example.com",
        "workPhone": "12345678",
        "address": "Testvej 1",
        "postalCode": "1234",
    }

    expected_address_visibility = {
        "facet": "visibility",
        "bvn": "Intern",
        "title": "Må vises internt",
        "scope": "INTERNAL",
    }

    def test_dar_address_visibility(self):
        """Verify that we use the correct visibility class when creating or updating
        employee addresses of the type 'Adresse' (= postal addresses.)"""
        instance = self.get_instance({})
        with patch.object(instance, "ensure_class_in_facet") as ensure_class:
            # Make sure "DAR" returns a "DAR UUID" so we trigger an update of the
            # "Adresse" address type (a postal address.)
            with patch(
                "integrations.opus.opus_diff_import.dawa_helper.dawa_lookup",
                return_value="dar-address-uuid",
            ):
                with patch.object(instance, "_perform_address_update"):
                    instance._update_employee_address("mo_uuid", self.opus_employee)
                    assert (
                        ensure_class.call_args.kwargs
                        == self.expected_address_visibility
                    )


if __name__ == "__main__":
    unittest.main()
