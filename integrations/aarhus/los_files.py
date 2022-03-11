import codecs
import csv
import os
import warnings
from abc import ABC
from datetime import datetime
from ftplib import FTP
from io import BytesIO
from io import StringIO
from operator import itemgetter
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import config
import pydantic
from more_itertools import one
from pydantic import BaseModel
from pydantic import parse_obj_as
from ra_utils.apply import apply


T = TypeVar("T")


class FileSet(ABC):
    def __init__(self):
        super().__init__()
        self._settings = config.get_config()

    def get_import_filenames(self) -> List[str]:
        raise NotImplementedError("must be implemented by subclass")

    def get_modified_datetime(self, filename: str) -> datetime:
        raise NotImplementedError("must be implemented by subclass")

    def read_file(self, filename: str) -> List[str]:
        raise NotImplementedError("must be implemented by subclass")

    def write_file(self, filename: str, stream: StringIO, folder: Optional[str] = None):
        raise NotImplementedError("must be implemented by subclass")


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
            ftp.cwd(self._settings.ftp_folder)
            return ftp

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


class FSFileSet(FileSet):
    def _get_import_file_path(self, filename: str):
        return os.path.join(self._settings.import_csv_folder, filename)

    def _get_export_file_path(self, filename: str):
        return os.path.join(self._settings.queries_dir, filename)

    def get_import_filenames(self) -> List[str]:
        return os.listdir(self._settings.import_csv_folder)

    def get_modified_datetime(self, filename: str) -> datetime:
        path = self._get_import_file_path(filename)
        if os.path.exists(path):
            modified_time = datetime.fromtimestamp(os.path.getmtime(path))
            return modified_time
        raise ValueError("%r does not exist" % path)

    def read_file(self, filename: str) -> List[str]:
        path = self._get_import_file_path(filename)
        if os.path.exists(path):
            with open(path) as f:
                return f.readlines()
        return []

    def write_file(self, filename: str, stream: StringIO, folder: Optional[str] = None):
        if folder is not None:
            warnings.warn(
                "%r.write_file received unused `folder` argument (= %r)"
                % (self.__class__.__name__, folder)
            )
        path = self._get_export_file_path(filename)
        with open(path, "w") as f:
            stream.seek(0)
            return f.writelines(stream.getvalue())


def get_fileset_implementation() -> Union[FTPFileSet, FSFileSet]:
    settings = config.get_config()
    if settings.import_csv_folder:
        return FSFileSet()
    else:
        return FTPFileSet()


def parse_filenames(
    filenames: Iterable[str], prefix: str, last_import: datetime
) -> List[Tuple[str, datetime]]:
    """
    Get valid filenames matching a prefix and date newer than last_import

    All valid filenames are on the form: {{prefix}}_20210131_221600.csv
    """

    def parse_filepath(filepath: str) -> Tuple[str, datetime]:
        date_part = filepath[-19:-4]
        parsed_datetime = datetime.strptime(date_part, "%Y%m%d_%H%M%S")

        return filepath, parsed_datetime

    filtered_names = filter(lambda x: x.startswith(prefix), filenames)
    parsed_names = map(parse_filepath, filtered_names)
    # Only use files that are newer than last import
    new_files = filter(
        apply(lambda filepath, filedate: filedate > last_import), parsed_names
    )
    sorted_filenames = sorted(new_files, key=itemgetter(1))
    return sorted_filenames


def parse_csv(lines: List[str], model: BaseModel) -> List[BaseModel]:
    def strip_empty(val: dict):
        return {k: v for k, v in val.items() if v != ""}

    reader = csv.DictReader(lines, delimiter=";")
    cleaned = list(map(strip_empty, reader))
    try:
        return parse_obj_as(List[model], cleaned)  # type: ignore
    except pydantic.ValidationError as e:
        raise ValueError("could not parse %r" % cleaned) from e


def read_csv(filename: str, model: T) -> List[T]:
    """Read CSV file from FTP into list of model objects"""
    print(f"Processing {filename}")
    fileset = get_fileset_implementation()
    lines = fileset.read_file(filename)
    return parse_csv(lines, model)  # type: ignore
