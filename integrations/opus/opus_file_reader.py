import datetime
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

import fs
from google.cloud import storage
from more_itertools import one

from exporters.utils.load_settings import load_settings


class OpusReaderInterface(ABC):
    @abstractmethod
    def list_opus_files(self) -> List[str]:
        raise NotImplementedError()

    @abstractmethod
    def read_file(self, blob_name: str) -> str:
        raise NotImplementedError()

    def map_dates(self, dump_list) -> Dict[datetime.datetime, str]:
        """Transforms the list of opus_dumps to a dictionary with dates as keys"""
        dumps = {}
        for opus_dump in dump_list:
            date_part = opus_dump.name[4:18]
            export_time = datetime.datetime.strptime(date_part, "%Y%m%d%H%M%S")
            dumps[export_time] = opus_dump
        return dumps

    def read_latest(self):
        all_files = self.list_opus_files()
        latest_date = max(all_files.keys())
        return self.read_file(all_files[latest_date])


class GcloudOpusReader(OpusReaderInterface):
    def __init__(self, settings):
        settings = settings
        bucket_name = settings["integrations.opus.gcloud_bucket_name"]
        self.client = storage.Client()
        self.bucket = storage.Bucket(self.client, bucket_name)

    def list_opus_files(self) -> Dict[datetime.datetime, str]:
        all_files = list(self.client.list_blobs(self.bucket))
        for f in all_files:
            f.name = f.name.replace("production/", "")
        return self.map_dates(all_files)

    def read_file(self, blob) -> str:
        return blob.download_as_text()


class SMBOpusReader(OpusReaderInterface):
    def __init__(self, settings):
        self.settings = settings
        user = self.settings["integrations.opus.smb_user"]
        password = self.settings["integrations.opus.smb_password"]
        smb_host = self.settings["integrations.opus.smb_host"]
        self.smb_fs = fs.open_fs(f"smb://{user}:{password}@{smb_host}")

    def list_opus_files(self) -> Dict[datetime.datetime, str]:
        all_files = self.smb_fs.glob("*.xml")
        return self.map_dates(x.info for x in all_files)

    def read_file(self, glob) -> str:
        f = one(self.smb_fs.glob(glob.name))
        return self.smb_fs.readtext(f.path)


class LocalOpusReader(OpusReaderInterface):
    def __init__(self, settings):
        self.settings = settings

    def list_opus_files(self) -> Dict[datetime.datetime, Path]:
        dump_path = Path(self.settings["integrations.opus.import.xml_path"])
        return self.map_dates(dump_path.glob("*.xml"))

    def read_file(self, filename) -> str:
        return filename.read_text()


def get_opus_filereader(settings: Optional[Dict] = None) -> OpusReaderInterface:
    """Get the correct opus reader interface based on values from settings."""
    settings = settings or load_settings()
    if settings.get("integrations.opus.gcloud_bucket_name"):
        return GcloudOpusReader(settings)
    if settings.get("integrations.opus.smb_host"):
        return SMBOpusReader(settings)
    return LocalOpusReader(settings)


if __name__ == "__main__":
    ofr = get_opus_filereader()
    print(ofr.list_opus_files())
