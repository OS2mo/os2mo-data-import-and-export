from google.cloud import storage
import fs
from exporters.utils.load_settings import load_settings
from pathlib import Path
import datetime
class opus_reader_gcloud:
    def __init__(self):
        settings = load_settings()
        bucket_name = settings["gcloud.bucket_name"]
        self.client = storage.Client()
        self.bucket = storage.Bucket(self.client, bucket_name)

    def list_files(self):
        all_files = self.client.list_blobs(self.bucket)
        return [f.name.replace('production/', '') for f in all_files]

    def read_file(self, blob):
        return blob.download_as_text()


class opus_reader_smb:
    def __init__(self):
        self.settings = load_settings()
        user = self.settings['Integrations.opus.smb_user']
        password = self.settings['Integrations.opus.smb_password']
        smb_host = self.settings['Integrations.opus.smb_host']
        self.smb_fs = fs.open_fs(f'smb://{user}:{password}@{smb_host}') 

    def list_files(self):
        all_files = self.smb_fs.glob('*.xml')
        return [f.path for f in all_files]

    def read_file(self, path):
        return smb_fs.readtext(path)
class opus_reader_local():
    def __init__(self):
        self.settings = load_settings()

    def list_files(self):
        dump_path = Path(self.settings['integrations.opus.import.xml_path'])
        return dump_path.glob('*.xml')

    def read_file(self, filename):
        return filename.read_text() 
 

class ofr():
    def __init__(self):
        settings = load_settings()
        if settings.get('gcloud.bucket_name'):
            self.opus_file_reader = opus_reader_gcloud()
        elif settings.get('ingetrations.opus.smb_host'):
            self.opus_file_reader = opus_reader_smb()
        else:
            self.opus_file_reader = opus_reader_local()

    def list_files(self):
        dumps = {}
        dump_list = self.opus_file_reader.list_files()
        for opus_dump in dump_list:
            date_part = opus_dump.name[4:18]
            export_time = datetime.datetime.strptime(date_part, '%Y%m%d%H%M%S')
            dumps[export_time] = opus_dump
        return dumps
    
    def read_file(self, filename):
        return self.opus_file_reader.read_file(filename)
if __name__ == "__main__":
    print(ofr().list_files())
