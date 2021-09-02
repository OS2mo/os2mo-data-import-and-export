from datetime import datetime
from io import BytesIO
from io import StringIO
from operator import itemgetter
from unittest import mock

import util
from hypothesis import given
from hypothesis import strategies as st
from more_itertools import one


class TestParseFilenames:
    def test_parses_prefix_correctly(self):
        filenames = [
            "xxxxOrg_nye_20210128_123742.csv",
            "Org_nye_20210128_123742.csv",
            "garbagebagasdasdads",
        ]

        expected_filename = "Org_nye_20210128_123742.csv"

        actual = util.parse_filenames(filenames, "Org_nye", datetime.min)

        actual_filename, _ = one(actual)

        assert expected_filename == actual_filename

    def test_filters_dates_correctly(self):
        filenames = [
            "Org_nye_20200128_123742.csv",
            "Org_nye_20201028_123742.csv",
            "Org_nye_20210128_123742.csv",
        ]

        expected = [
            ("Org_nye_20201028_123742.csv", datetime(2020, 10, 28, 12, 37, 42)),
            ("Org_nye_20210128_123742.csv", datetime(2021, 1, 28, 12, 37, 42)),
        ]

        actual = util.parse_filenames(
            filenames, "Org_nye", datetime(2020, 6, 1, 0, 0, 0)
        )

        assert expected == actual

    @given(
        st.permutations(
            [
                "Org_nye_20210101_000000.csv",
                "Org_nye_20200101_000000.csv",
                "Org_nye_20190101_000000.csv",
                "Org_nye_20180101_000000.csv",
            ]
        )
    )
    def test_sorts_output(self, filenames):
        expected_dates = [
            datetime(2018, 1, 1),
            datetime(2019, 1, 1),
            datetime(2020, 1, 1),
            datetime(2021, 1, 1),
        ]

        actual = util.parse_filenames(filenames, "Org_nye", datetime.min)
        actual_dates = list(map(itemgetter(1), actual))

        assert expected_dates == actual_dates


class TestConvertBytesIOToStringIO:
    @given(st.text())
    def test_converts_ok(self, text):
        stringio = StringIO(text)
        bytesio = util.convert_stringio_to_bytesio(stringio)
        assert isinstance(bytesio, BytesIO)


class TestWriteCSVToFTP:
    @given(st.text(), st.text(), st.text())
    def test_makes_ftp_calls(self, filename, text, folder):
        csv_stream = StringIO(text)
        mock_ftp = mock.MagicMock()
        with mock.patch.object(util, "get_ftp_connector", return_value=mock_ftp):
            util.write_csv_to_ftp(filename, csv_stream, folder)
            mock_ftp.cwd.assert_called_once_with(folder)
            mock_ftp.storlines.assert_called_once_with(f"STOR {filename}", mock.ANY)
            mock_ftp.close.assert_called_once()
            csv_bytes = mock_ftp.storlines.call_args.args[1]
            csv_stream_as_bytes = util.convert_stringio_to_bytesio(csv_stream)
            assert csv_bytes.getvalue() == csv_stream_as_bytes.getvalue()
