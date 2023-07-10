import codecs
import logging
from abc import ABC
from datetime import datetime
from ftplib import FTP
from io import BytesIO, StringIO
from typing import List, Optional, Tuple, TypeVar

import config
import paramiko
from more_itertools import one
from paramiko.client import SSHClient
from paramiko.sftp_client import SFTPClient

T = TypeVar("T")

logger = logging.getLogger(__name__)


class FileSet(ABC):
    def __init__(self):
        super().__init__()
        self._settings = config.get_employee_phone_book_settings()

    def get_import_filenames(self) -> List[str]:
        raise NotImplementedError("must be implemented by subclass")

    def get_modified_datetime(self, filename: str) -> datetime:
        raise NotImplementedError("must be implemented by subclass")

    def read_file(self, filename: str) -> List[str]:
        raise NotImplementedError("must be implemented by subclass")

    def write_file(self, filename: str, stream: StringIO, folder: Optional[str] = None):
        raise NotImplementedError("must be implemented by subclass")

    def _convert_stringio_to_bytesio(
        self, output: StringIO, encoding: str = "utf-8"
    ) -> BytesIO:
        """Convert StringIO object `output` to a BytesIO object using `encoding`"""
        output.seek(0)
        bytes_output = BytesIO()
        bytes_writer = codecs.getwriter(encoding)(bytes_output)
        bytes_writer.write(output.getvalue())
        bytes_output.seek(0)
        return bytes_output


class FTPFileSet(FileSet):
    def _get_ftp_connector(self) -> FTP:
        try:
            ftp = FTP(self._settings.ftp_url)
        except Exception as e:
            raise config.ImproperlyConfigured(
                "cannot connect to FTP server %r"
                % getattr(self._settings, "ftp_url", None)
            ) from e
        else:
            ftp.encoding = "utf-8"
            ftp.login(user=self._settings.ftp_user, passwd=self._settings.ftp_pass)
            return ftp

    def get_import_filenames(self) -> List[str]:
        ftp = self._get_ftp_connector()
        filenames = ftp.nlst()
        return filenames

    def get_modified_datetime(self, filename: str) -> datetime:
        """Read the 'modified' field from an FTP file"""
        ftp = self._get_ftp_connector()
        files = ftp.mlsd()
        found_file = one(filter(lambda x: x[0] == filename, files))
        filename, facts = found_file
        # String is on the form: "20210323153241.448"
        modify_string = facts["modify"][:-4]
        return datetime.strptime(modify_string, "%Y%m%d%H%M%S")

    def read_file(self, filename: str) -> List[str]:
        ftp = self._get_ftp_connector()
        lines: List[str] = []
        ftp.retrlines(f"RETR {filename}", lines.append)
        return lines

    def write_file(self, filename: str, stream: StringIO, folder: Optional[str] = None):
        ftp = self._get_ftp_connector()
        if folder:
            ftp.cwd(folder)
        result = ftp.storlines(
            f"STOR {filename}", self._convert_stringio_to_bytesio(stream)
        )
        ftp.close()
        return result


class SFTPFileSet(FileSet):
    def _get_sftp_connector(self) -> Tuple[SSHClient, SFTPClient]:
        try:
            ssh = paramiko.SSHClient()

            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        except Exception as e:
            raise config.ImproperlyConfigured(
                "cannot connect to FTP server %r"
                % getattr(self._settings, "ftp_url", None)
            ) from e
        else:
            try:
                ssh.connect(
                    hostname=self._settings.ftp_url,
                    port=self._settings.ftp_port,
                    username=self._settings.ftp_user,
                    password=self._settings.ftp_pass,
                )

                sftp = ssh.open_sftp()
                if not sftp:
                    raise

            except Exception as e:
                raise config.ImproperlyConfigured(
                    "cannot connect to FTP server %r"
                    % getattr(self._settings, "ftp_url", None)
                ) from e
            else:
                sftp.chdir(self._settings.ftp_folder)
                return ssh, sftp

    def get_import_filenames(self) -> List[str]:
        ssh, sftp = self._get_sftp_connector()
        filenames = sftp.listdir()
        sftp.close()
        ssh.close()
        return filenames

    def get_modified_datetime(self, filename: str) -> datetime:
        """Read the 'modified' field from an FTP file"""
        ssh, sftp = self._get_sftp_connector()
        file_stats = sftp.stat(filename)
        sftp.close()
        ssh.close()
        modified_time = file_stats.st_mtime
        if not modified_time:
            raise AttributeError
        return datetime.fromtimestamp(float(modified_time))

    def read_file(self, filename: str) -> List[str]:
        ssh, sftp = self._get_sftp_connector()
        file = sftp.file(filename=filename, mode="r")
        lines = file.readlines()
        file.close()
        sftp.close()
        ssh.close()
        return lines

    def write_file(self, filename: str, stream: StringIO, folder: Optional[str] = None):
        ssh, sftp = self._get_sftp_connector()
        if folder:
            try:
                sftp.chdir(folder)
            except IOError:
                sftp.mkdir(folder)
                sftp.chdir(folder)
        result = sftp.putfo(
            fl=self._convert_stringio_to_bytesio(stream), remotepath=filename
        )
        sftp.close()
        ssh.close()
        return result
