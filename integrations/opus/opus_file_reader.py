import datetime
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Dict
from typing import Optional

import click
import fs
from google.cloud import storage
from more_itertools import one
from ra_utils.load_settings import load_settings
from retrying import retry


class OpusReaderInterface(ABC):
    @abstractmethod
    def list_opus_files(self) -> Dict[datetime.datetime, str]:
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


retry_args = {
    "stop_max_attempt_number": 7,
    "wait_fixed": 2000,
}


class SMBOpusReader(OpusReaderInterface):
    @retry(**retry_args)
    def __init__(self, settings):
        self.settings = settings
        user = self.settings["integrations.opus.smb_user"]
        password = self.settings["integrations.opus.smb_password"]
        smb_host = self.settings["integrations.opus.smb_host"]
        self.smb_fs = fs.open_fs(f"smb://{user}:{password}@{smb_host}")

    @retry(**retry_args)
    def list_opus_files(self) -> Dict[datetime.datetime, str]:
        all_files = self.smb_fs.glob("*.xml")
        return self.map_dates(x.info for x in all_files)

    @retry(**retry_args)
    def read_file(self, glob) -> str:
        f = one(self.smb_fs.glob(glob.name))
        return self.smb_fs.readtext(f.path)


class LocalOpusReader(OpusReaderInterface):
    def __init__(self, settings):
        self.settings = settings

    def list_opus_files(self) -> Dict[datetime.datetime, str]:
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


@click.group()
def cli():
    """CLI for reading opus-files"""
    pass


@cli.command()
def read_last():
    """Read latest opus-file"""
    ofr = get_opus_filereader()
    click.echo(ofr.read_latest())


@cli.command()
def list_files():
    """Show dates of all opus-files"""
    ofr = get_opus_filereader()
    dumps = ofr.list_opus_files()
    dates = sorted(dumps.keys())
    for date in dates:
        click.echo(date)


@cli.command()
@click.option("--date", type=click.DateTime())
def read_file(date):
    """Read opus-file from specific date. If no date is supplied show all available dates"""
    ofr = get_opus_filereader()
    dumps = ofr.list_opus_files()

    if not date:
        dates = sorted(dumps.keys())
        for date in dates:
            click.echo(date)
        click.echo(f'Choose from above and provide as parameter, eg. --date="{date}"')
    else:
        click.echo(ofr.read_file(dumps[date]))


if __name__ == "__main__":
    cli()
