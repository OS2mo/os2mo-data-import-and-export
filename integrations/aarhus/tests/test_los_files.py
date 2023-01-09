from io import BytesIO
from io import StringIO
from unittest import mock

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from .helpers import mock_config


class TestFTPFileSet:
    @given(st.text())
    def test_convert_stringio_to_bytesio(self, text):
        fileset = self._get_ftpfileset()
        stringio = StringIO(text)
        bytesio = fileset._convert_stringio_to_bytesio(stringio)
        assert isinstance(bytesio, BytesIO)

    @settings(deadline=None)
    @given(st.text(), st.text(), st.text())
    def test_makes_ftp_calls(self, filename, text, folder):
        fileset = self._get_ftpfileset()
        csv_stream = StringIO(text)
        mock_ftp = mock.MagicMock()
        with mock.patch.object(fileset, "_get_ftp_connector", return_value=mock_ftp):
            fileset.write_file(filename, csv_stream, folder)
            if folder:
                mock_ftp.cwd.assert_called_once_with(folder)
            mock_ftp.storlines.assert_called_once_with(f"STOR {filename}", mock.ANY)
            mock_ftp.close.assert_called_once()
            csv_bytes = mock_ftp.storlines.call_args.args[1]
            csv_stream_as_bytes = fileset._convert_stringio_to_bytesio(csv_stream)
            assert csv_bytes.getvalue() == csv_stream_as_bytes.getvalue()

    def _get_ftpfileset(self):
        with mock_config(import_csv_folder=None):
            import los_files

            return los_files.FTPFileSet()


class TestSFTPFileSet:
    @given(st.text())
    def test_convert_stringio_to_bytesio(self, text):
        with mock_config(ftp_ssh_key_path="ftp_ssh_key_path"):
            fileset = self._get_sftpfileset()
            stringio = StringIO(text)
            bytesio = fileset._convert_stringio_to_bytesio(stringio)
            assert isinstance(bytesio, BytesIO)

    @settings(deadline=None)
    @given(st.text(), st.text(), st.text())
    def test_makes_sftp_calls(self, filename, text, folder):
        with mock_config(
            ftp_ssh_key_path="ftp_ssh_key_path",
            ftp_ssh_key_pass="ftp_ssh_key_pass",
        ):
            fileset = self._get_sftpfileset()
            csv_stream = StringIO(text)
            mock_sftp = mock.MagicMock()
            mock_ssh = mock.MagicMock()
            with mock.patch.object(
                fileset, "_get_sftp_connector", return_value=(mock_ssh, mock_sftp)
            ):
                fileset.write_file(filename, csv_stream, folder)
                if folder:
                    mock_sftp.chdir.assert_called_once_with(folder)
                mock_sftp.putfo.assert_called_once_with(
                    fl=mock.ANY, remotepath=filename
                )
                mock_sftp.close.assert_called_once()
                csv_bytes = mock_sftp.putfo.call_args.kwargs["fl"]
                csv_stream_as_bytes = fileset._convert_stringio_to_bytesio(csv_stream)
                assert csv_bytes.getvalue() == csv_stream_as_bytes.getvalue()

    def _get_sftpfileset(self):
        with mock_config(import_csv_folder=None, ftp_ssh_key_path="ftp_ssh_key_path"):
            import los_files

            return los_files.SFTPFileSet()
