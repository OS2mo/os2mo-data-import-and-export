import csv
from datetime import datetime
from typing import List
from unittest import mock
from uuid import UUID

import los_files
import los_org
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from .helpers import HelperMixin
from .helpers import mock_config
from .strategies import csv_buf_from_model
from .strategies import text_except


class TestConsolidatePayloads:
    def test_consolidate_identical(self):
        """Should correctly consolidate identical consecutive payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 123, "validity": {"from": "2011-01-01", "to": "2011-12-31"}},
            {"val": 123, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
            {"val": 123, "validity": {"from": "2013-01-01", "to": "2013-12-31"}},
            {"val": 123, "validity": {"from": "2014-01-01", "to": "2014-12-31"}},
        ]

        expected = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2014-12-31"}}
        ]

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_non_identical(self):
        """Should handle two consecutive non-identical payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 456, "validity": {"from": "2011-01-01", "to": "2011-12-31"}},
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_non_consecutive(self):
        """Should handle non-consecutive payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 123, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_mixed(self):
        """Should handle a mix between identical and non-identical payloads"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {"val": 123, "validity": {"from": "2011-01-01", "to": "2011-12-31"}},
            {"val": 456, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
            {"val": 789, "validity": {"from": "2013-01-01", "to": "2013-12-31"}},
            {"val": 789, "validity": {"from": "2014-01-01", "to": "2014-12-31"}},
        ]

        expected = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2011-12-31"}},
            {"val": 456, "validity": {"from": "2012-01-01", "to": "2012-12-31"}},
            {"val": 789, "validity": {"from": "2013-01-01", "to": "2014-12-31"}},
        ]

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_differing(self):
        """Should handle cases where payloads have differing keys"""
        payloads = [
            {"val": 123, "validity": {"from": "2010-01-01", "to": "2010-12-31"}},
            {
                "val": 123,
                "val2": 456,
                "validity": {"from": "2011-01-01", "to": "2011-12-31"},
            },
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_single_element(self):
        """Should return same single payload"""
        payloads = [
            {"val": 456, "validity": {"from": "2011-01-01", "to": "2011-12-31"}}
        ]

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual

    def test_consolidate_empty(self):
        """Should trivially handle empty input"""
        payloads = []

        expected = payloads

        actual = los_org.OrgUnitImporter.consolidate_payloads(payloads)

        assert expected == actual


class TestParseOrgUnitCSV:
    @given(csv_buf_from_model(model=los_org.OrgUnit))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_org.OrgUnit)


class TestHandleInitial(HelperMixin):
    @settings(deadline=None)
    @given(st.builds(los_org.OrgUnit), st.uuids())
    def test_handle_initial(self, instance: los_org.OrgUnit, dar_uuid: UUID):
        importer = los_org.OrgUnitImporter()
        with self._mock_dar_lookup((instance.post_address, dar_uuid)):
            with self._mock_read_csv(instance):
                with self._mock_get_client_session():
                    with self._mock_create_details() as mock_create_details:
                        self._run_until_complete(
                            importer.handle_initial("unused_filename.csv")
                        )
                        mock_create_details.assert_called_once()
                        heads = list(mock_create_details.call_args.args[1])
                        assert heads == [
                            {
                                "type": "org_unit",
                                "uuid": str(instance.org_uuid),
                                "user_key": instance.bvn,
                                "parent": {"uuid": str(instance.parent_uuid)},
                                "validity": {
                                    "from": str(instance.start_date),
                                    "to": str(instance.end_date),
                                },
                            }
                        ]


class TestHandleCreate(HelperMixin):
    @given(st.builds(los_org.OrgUnit), st.uuids())
    def test_handle_create(self, instance: los_org.OrgUnit, dar_uuid: UUID):
        importer = los_org.OrgUnitImporter()
        with self._mock_dar_lookup((instance.post_address, dar_uuid)):
            with self._mock_read_csv(instance):
                with self._mock_get_client_session():
                    with self._mock_create_details() as mock_create_details:
                        self._run_until_complete(
                            importer.handle_create("unused_filename.csv")
                        )
                        mock_create_details.assert_called_once()


class TestHandleEdit(HelperMixin):
    @given(st.builds(los_org.OrgUnit), st.uuids(), st.datetimes())
    def test_handle_edit(
        self,
        instance: los_org.OrgUnit,
        dar_uuid: UUID,
        filedate: datetime,
    ):
        importer = los_org.OrgUnitImporter()
        with self._mock_dar_lookup((instance.post_address, dar_uuid)):
            with self._mock_read_csv(instance):
                with self._mock_get_client_session():
                    with self._mock_create_details() as mock_create_details:
                        with self._mock_edit_details() as mock_edit_details:
                            with self._mock_lookup_organisationfunktion():
                                self._run_until_complete(
                                    importer.handle_edit(
                                        "unused_filename.csv", filedate
                                    )
                                )
                                mock_create_details.assert_called_once()
                                mock_edit_details.assert_called_once()


class TestHandleAddresses(HelperMixin):
    @given(st.builds(los_org.OrgUnit))
    def test_finds_failed_addresses(self, instance: los_org.OrgUnit):
        importer = los_org.OrgUnitImporter()
        expected_filename = "filename.csv"

        # Patch DAR lookup function to indicate a failed address lookup for any
        # address passed to it.
        with self._mock_dar_lookup((instance.post_address, None)):
            # Patch `write_failed_addresses` so we can test its arguments
            with mock.patch.object(importer, "write_failed_addresses") as mock_write:
                # Run method under test
                self._run_until_complete(
                    importer.handle_addresses([instance], "filename.csv")
                )
                # Assert args to mocked `write_failed_addresses`
                expected_failed = [
                    los_org.FailedDARLookup(
                        org_uuid=instance.org_uuid,
                        post_address=instance.post_address,
                    )
                ]
                mock_write.assert_called_once_with(expected_failed, expected_filename)

    @given(st.lists(st.builds(los_org.OrgUnit, post_address=st.none())))
    def test_does_nothing_on_empty_addresses(self, org_units: List[los_org.OrgUnit]):
        importer = los_org.OrgUnitImporter()
        with self._mock_dar_lookup(("", None)):
            with mock.patch.object(importer, "write_failed_addresses"):
                self._run_until_complete(
                    importer.handle_addresses(org_units, "filename.csv")
                )
                # Assert that we returned early because we only saw empty
                # addresses in `org_units`.
                assert importer.dar_cache == {}


class TestWriteFailedAddresses:
    # Construct a `FailedDARLookup` where the `post_address` does not contain
    # ASCII NUL characters nor the CSV delimiter character
    # (which in this case is "#" as per customer spec, see
    # `OrgUnitImporter.write_failed_addresses`.)
    @settings(max_examples=1000, deadline=None)
    @given(st.builds(los_org.FailedDARLookup, post_address=text_except(chr(0), "#")))
    def test_writes_csv_to_ftp(self, instance: los_org.FailedDARLookup):
        # Patch `config.get_config` so `write_failed_addresses` sees
        # `MockConfig.queries_dir` instead of actual setting.
        with mock_config(queries_dir="/tmp/"):
            # Patch `FTPFileSet.write_file` so we can test its arguments
            with mock.patch.object(los_files.FTPFileSet, "write_file") as mock_write:
                # Run `write_failed_addresses`
                importer = los_org.OrgUnitImporter()
                importer.write_failed_addresses([instance], "filename.csv")
                # Assert our mocked `write_file` got the expected args
                mock_write.assert_called_once()
                # First arg is filename, second arg is CSV file (as bytes)
                filename, csv_buf = mock_write.call_args_list[0].args
                # Filename is appended to output filename
                assert filename == "failed_addr_filename.csv"
                # Assert contents of the CSV file built
                csv_rows = list(csv.DictReader(csv_buf, delimiter="#"))
                assert len(csv_rows) == 1
                assert csv_rows[0] == {
                    "org_uuid": str(instance.org_uuid),
                    "post_address": instance.post_address,
                }
