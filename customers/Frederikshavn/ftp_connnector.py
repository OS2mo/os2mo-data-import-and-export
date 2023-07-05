import codecs
import logging
from abc import ABC
from datetime import datetime
from ftplib import FTP
from io import BytesIO, StringIO
from typing import List, Optional, TypeVar

from more_itertools import one

import config

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
