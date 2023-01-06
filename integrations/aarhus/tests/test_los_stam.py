import uuid
from datetime import datetime
from unittest import mock

import los_files
import los_stam
import uuids
from aiohttp import ClientResponseError
from aiohttp.http_exceptions import HttpBadRequest
from hypothesis import given
from mox_helpers.mox_helper import ElementNotFound
from pydantic import Field

from .helpers import HelperMixin
from .helpers import mock_config
from .helpers import mock_create_mox_helper
from .strategies import csv_buf_from_model


class MockStamCSV(los_stam.StamCSV):
    mock_field: str = Field(default="testing testing")

    @staticmethod
    def get_filename() -> str:
        return "mock-filename.csv"

    @staticmethod
    def get_facet_bvn() -> str:
        return "mock_facet_bvn"

    @property
    def bvn(self) -> str:
        return "bvn"

    @property
    def title(self) -> str:
        return "title"

    @property
    def class_uuid(self) -> uuid.UUID:
        return uuid.uuid4()


class TestStamImporterLoadCSVIfNewer(HelperMixin):
    """Test `los_stam.StamImporter._load_csv_if_newer`."""

    _datetime_last_imported = datetime(2020, 1, 1)
    _datetime_last_modified = datetime(2020, 1, 2)

    def test_returns_csv_if_file_is_newer(self):
        # Pretend that `get_modified_datetime_for_file` finds a file, and that
        # `read_csv` returns its parsed contents.
        with self._mock_get_modified_datetime(
            return_value=self._datetime_last_modified
        ):
            with self._mock_read_csv(MockStamCSV()):
                result = self._run_load_csv_if_newer()
                assert len(result) == 1
                assert isinstance(result[0], MockStamCSV)
                assert result[0].mock_field == MockStamCSV().mock_field

    def test_returns_nothing_if_file_is_older(self):
        # Pretend that `get_modified_datetime_for_file` finds a file, but its
        # modified date is equal to the date of the last import run. In that
        # case, return nothing.
        with self._mock_get_modified_datetime(
            return_value=self._datetime_last_imported
        ):
            result = self._run_load_csv_if_newer()
            assert result is None

    def test_handles_missing_file(self):
        # Pretend that no file is found by `get_modified_datetime_for_file`.
        with self._mock_get_modified_datetime(side_effect=ValueError()):
            result = self._run_load_csv_if_newer()
            assert result is None

    def test_create_classes_from_csv(self):
        # Pretend we are adding exactly one LoRa class based on a single-item CSV file
        rows = [MockStamCSV()]

        with mock_config():
            instance = los_stam.StamImporter(self._datetime_last_imported)
            with mock_create_mox_helper(los_stam) as mh:
                mox_helper = mh.return_value
                mox_helper.read_element_klassifikation_facet.return_value = uuid.uuid4()
                self._run_until_complete(
                    instance._create_classes_from_csv(MockStamCSV, rows)
                )
                # Assert that we add one class in the facet
                mox_helper.insert_klassifikation_klasse.assert_called_once()

                # Assert that we convert the facet UUID to string, and we use the right
                # facet UUID.
                (
                    payload,
                    class_uuid,
                ) = mox_helper.insert_klassifikation_klasse.call_args.args
                expected_facet_uuid = str(
                    mox_helper.read_element_klassifikation_facet.return_value
                )
                actual_facet_uuid = payload["relationer"]["facet"][0]["uuid"]
                assert expected_facet_uuid == actual_facet_uuid

                # Assert that we convert the class UUID to string
                assert isinstance(class_uuid, str)

    def test_unpublish_handles_already_unpublished_class(self, capsys):
        # When trying to unpublish a LoRa class which has already been unpublished in a
        # a previous LOS import run, make sure that we handle the error raised by
        # `mox_helper._update`. (#54283)

        # In this test, we mock the `_search` method of `MoxHelper` to return a random
        # class UUID, while we process an empty "CSV file".
        existing_class_uuid = str(uuid.uuid4())
        empty_csv_file = []

        # The expected behavior is that `_create_classes_from_csv` will try to unpublish
        # the class which is in MO but not in the CSV file.
        # However, doing this more than once will trigger a `ClientResponseError`.
        side_effect = ClientResponseError(request_info=None, history=None)
        side_effect.status = HttpBadRequest.code

        # Arrange
        with mock_config():
            instance = los_stam.StamImporter(self._datetime_last_imported)
            with mock_create_mox_helper(los_stam) as mh:
                mox_helper = mh.return_value
                mox_helper._search.return_value = [existing_class_uuid]
                mox_helper._update.side_effect = side_effect

                # Act
                self._run_until_complete(
                    instance._create_classes_from_csv(MockStamCSV, empty_csv_file)
                )

                # Assert that we tried to unpublish the class in question
                mox_helper._update.assert_called_once()
                class_uuid = mox_helper._update.call_args.args[2]
                assert class_uuid == existing_class_uuid
                payload = mox_helper._update.call_args.args[3]
                assert payload["tilstande"]["klassepubliceret"][0]["publiceret"] == "IkkePubliceret"

                # Assert that we logged the expected output to stdout
                captured = capsys.readouterr()
                assert captured.out == f"LoRa class UUID('{existing_class_uuid}') was already unpublished\n"

    def test_get_or_create_facet_existing_facet(self):
        with mock_config():
            with mock_create_mox_helper(los_stam) as mh:
                expected_facet_uuid = uuid.uuid4()
                mox_helper = mh.return_value

                # Pretend that we find the facet UUID in LoRa (= it already exists)
                mox_helper.read_element_klassifikation_facet.return_value = (
                    expected_facet_uuid
                )

                result = self._run_until_complete(
                    los_stam.StamImporter._get_or_create_facet(MockStamCSV, mox_helper)
                )

                # Assert that we retrieve the facet UUID in LoRa by looking up the BVN
                mox_helper.read_element_klassifikation_facet.assert_called_once_with(
                    bvn=MockStamCSV.get_facet_bvn()
                )
                # Assert that we got the facet UUID that we mocked
                assert result == expected_facet_uuid

    def test_get_or_create_facet_new_facet(self):
        with mock_config():
            with mock_create_mox_helper(los_stam) as mh:
                mox_helper = mh.return_value
                expected_facet_uuid = uuids.uuid_gen(MockStamCSV.get_facet_bvn())

                # Pretend that looking up the facet by its BVN will fail
                mox_helper.read_element_klassifikation_facet.side_effect = (
                    ElementNotFound
                )

                result = self._run_until_complete(
                    los_stam.StamImporter._get_or_create_facet(MockStamCSV, mox_helper)
                )

                # Assert that we look for the facet UUID in LoRa by looking up the BVN
                mox_helper.read_element_klassifikation_facet.assert_called_once_with(
                    bvn=MockStamCSV.get_facet_bvn()
                )
                # Assert that we added a new facet to LoRa
                mox_helper.insert_klassifikation_facet.assert_called_once_with(
                    mock.ANY,
                    expected_facet_uuid,
                )
                # Assert that we got the facet UUID that we constructed ourselves
                assert result == expected_facet_uuid

    def _run_load_csv_if_newer(self):
        instance = los_stam.StamImporter(self._datetime_last_imported)
        return instance._load_csv_if_newer(MockStamCSV)


class TestParseEngagementstypeCSV:
    @given(csv_buf_from_model(model=los_stam.Engagementstype))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Engagementstype)


class TestParseEnhedstypeCSV:
    @given(csv_buf_from_model(model=los_stam.Enhedstype))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Enhedstype)


class TestParseITSystemCSV:
    @given(csv_buf_from_model(model=los_stam.ITSystem))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.ITSystem)


class TestParseStillingsbetegnelseCSV:
    @given(csv_buf_from_model(model=los_stam.Stillingsbetegnelse))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Stillingsbetegnelse)


class TestParseBVNStillingsbetegnelseCSV:
    @given(csv_buf_from_model(model=los_stam.BVNStillingsbetegnelse))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.BVNStillingsbetegnelse)


class TestParseLederansvarCSV:
    @given(csv_buf_from_model(model=los_stam.Lederansvar))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Lederansvar)


class TestParseLederniveauCSV:
    @given(csv_buf_from_model(model=los_stam.Lederniveau))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Lederniveau)


class TestParseLedertypeCSV:
    @given(csv_buf_from_model(model=los_stam.Ledertype))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Ledertype)


class TestParseTilknytningsrolleCSV:
    @given(csv_buf_from_model(model=los_stam.Tilknytningsrolle))
    def test_parse_raises_nothing(self, csv_buf):
        los_files.parse_csv(csv_buf.readlines(), los_stam.Tilknytningsrolle)
