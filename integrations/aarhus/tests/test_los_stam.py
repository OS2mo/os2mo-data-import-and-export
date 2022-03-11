from datetime import datetime

import los_files
import los_stam
from hypothesis import given
from pydantic import Field

from .helpers import HelperMixin
from .strategies import csv_buf_from_model


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

    def _run_load_csv_if_newer(self):
        return los_stam.StamImporter._load_csv_if_newer(
            MockStamCSV, self._datetime_last_imported
        )


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
