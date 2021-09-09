from datetime import datetime
from unittest import mock

import los_files
import los_stam
from pydantic import Field

from .helpers import HelperMixin


class MockStamCSV(los_stam.StamCSV):
    mock_field: str = Field(default="testing testing")

    @staticmethod
    def get_filename() -> str:
        return "mock-filename.csv"


class TestStamImporterLoadCSVIfNewer(HelperMixin):
    """Test `los_stam.StamImporter._load_csv_if_newer`."""

    _datetime_last_imported = datetime(2020, 1, 1)
    _datetime_last_modified = datetime(2020, 1, 2)

    def test_returns_csv_if_file_is_newer(self):
        # Pretend that `get_modified_datetime_for_file` finds a file, and that
        # `read_csv` returns its parsed contents.
        with self._mock_get_modified_datetime():
            with self._mock_read_csv(MockStamCSV()):
                result = self._run_load_csv_if_newer()
                assert len(result) == 1
                assert isinstance(result[0], MockStamCSV)
                assert result[0].mock_field == MockStamCSV().mock_field

    def test_returns_nothing_if_file_is_older(self):
        # Pretend that `get_modified_datetime_for_file` finds a file, but its
        # modified date is equal to the date of the last import run. In that
        # case, return nothing.
        with self._mock_get_modified_datetime(value=self._datetime_last_imported):
            result = self._run_load_csv_if_newer()
            assert result is None

    def test_handles_missing_file(self):
        # Pretend that no file is found by `get_modified_datetime_for_file`.
        with self._mock_get_modified_datetime(side_effect=ValueError()):
            result = self._run_load_csv_if_newer()
            assert result is None

    def _mock_get_modified_datetime(self, value=None, side_effect=None):
        return mock.patch.object(
            los_files.fileset,
            "get_modified_datetime",
            return_value=value or self._datetime_last_modified,
            side_effect=side_effect,
        )

    def _run_load_csv_if_newer(self):
        return los_stam.StamImporter._load_csv_if_newer(
            MockStamCSV, self._datetime_last_imported
        )
