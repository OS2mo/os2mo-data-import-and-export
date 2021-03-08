import unittest
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import xmltodict
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import datetimes, dictionaries, text, uuids
from parameterized import parameterized, parameterized_class

from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport, start_opus_diff


class OpusDiffImportTestbase(OpusDiffImport):
    @patch("integrations.opus.calculate_primary.MOPrimaryEngagementUpdater")
    @patch(
        "integrations.opus.calculate_primary.MOPrimaryEngagementUpdater.__init__",
        return_value=None,
    )
    def __init__(self, latest_date, primary_init_mock, mock_primary, *args, **kwargs):
        self.morahelper_mock = MagicMock()
        self.morahelper_mock.read_organisation.return_value = "org_uuid"
        self.morahelper_mock._mo_post.return_value.status_code = 201
        self.ensure_class_mock = (MagicMock(), MagicMock())
        self._add_klasse_to_lora = MagicMock()
        mock_primary.primary_types = {"non_primary": "test"}

        super().__init__(latest_date, *args, **kwargs)

    def _get_mora_helper(self, hostname, use_cache):
        return self.morahelper_mock
    
    def _ensure_class_in_lora(self, facet, klasse):
        return self.ensure_class_mock

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
        self.file1 = Path.cwd() / "integrations/opus/tests/ZLPETESTER_delta.xml"
        self.file2 = Path.cwd() / "integrations/opus/tests/ZLPETESTER2_delta.xml"
        self.expected_unit_count = 3
        self.expected_employee_count = 2
        self.expected_terminations = 1

        filter_ids = []
        self.units, self.employees = opus_helpers.file_diff(
            self.file1, self.file2, filter_ids
        )

    def test_file_diff(self):
        self.assertEqual(len(self.units), self.expected_unit_count)
        self.assertEqual(
            len(self.employees),
            self.expected_employee_count + self.expected_terminations,
        )

    @given(datetimes())
    def test_import_unit_count(self, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTest_counts(xml_date, ad_reader=None)
        diff.start_import(self.units, self.employees, include_terminations=True)
        self.assertEqual(diff.update_unit.call_count, self.expected_unit_count)

    @given(datetimes())
    def test_import_employee_count(self, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTest_counts(xml_date, ad_reader=None)
        diff.start_import(self.units, self.employees, include_terminations=True)
        self.assertEqual(diff.update_employee.call_count, self.expected_employee_count)

    @given(datetimes())
    def test_termination(self, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTest_counts(xml_date, ad_reader=None)
        diff.start_import(self.units, self.employees, include_terminations=True)
        self.assertEqual(diff.terminate_detail.call_count, self.expected_terminations*2)

    @patch("integrations.dawa_helper.dawa_lookup")
    @given(datetimes())
    def test_update_unit(self, dawa_helper_mock, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None)
        for unit in self.units:
            diff.update_unit(unit)
            calculated_uuid = opus_helpers.generate_uuid(unit["@id"])
            add_type_uuid, _ = diff.ensure_class_mock
            diff.helper._mo_post.assert_called_with(
                "details/create",
                {
                    "type": "address",
                    "value": dawa_helper_mock(),
                    "address_type": {"uuid": add_type_uuid},
                    "validity": {"from": xml_date.strftime("%Y-%m-%d"), "to": None},
                    "org_unit": {"uuid": str(calculated_uuid)},
                },
            )

    @patch("integrations.dawa_helper.dawa_lookup")
    @given(datetimes())
    def test_update_employee(self, dawa_helper_mock, xml_date):
        self.assertIsInstance(xml_date, datetime)
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None)
        diff.updater.primary_types = {"non_primary": "test"}
        diff.updater.set_current_person = MagicMock()
        diff.updater.recalculate_primary = MagicMock()
        with patch(
            "integrations.opus.opus_diff_import.OpusDiffImport._assert",
            return_value=None,
        ):
            for employee in self.employees:
                if employee.get('cpr'):
                    diff.update_employee(employee)
                    diff.helper._mo_post.assert_called_with(
                        "details/terminate",
                        {
                            "type": "manager",
                            "uuid": diff.helper._mo_lookup().__getitem__().__getitem__(),
                            "validity": {"to": xml_date.strftime("%Y-%m-%d")},
                        },
                    )
                else:
                    self.assertEqual(employee['@action'], "leave")

    @given(datetimes(), datetimes(), text(), uuids(), uuids())
    def test_perform_address_update_create(
        self, xml_date, fromdate, value, address_type_uuid, org_unit_uuid
    ):
        diff = OpusDiffImportTestbase(xml_date, ad_reader=None, employee_mapping={})
        address_type_uuid = str(address_type_uuid)
        args = {
            "address_type": {"uuid": address_type_uuid},
            "value": value,
            "validity": {"from": fromdate.strftime("%Y-%m-%d"), "to": None},
            "unit_uuid": str(org_unit_uuid),
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
            },
        )


if __name__ == "__main__":
    unittest.main()
