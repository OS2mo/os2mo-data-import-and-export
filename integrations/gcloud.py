from google.cloud import storage

from exporters.utils.load_settings import load_settings


class gcloud_reader:
    def __init__(self):
        settings = load_settings()
        bucket_name = settings["gcloud.bucket_name"]
        self.client = storage.Client()
        self.bucket = storage.Bucket(self.client, bucket_name)

    def list_files(self):
        return self.client.list_blobs(self.bucket)

    def read_file(self, blob):
        return blob.download_as_text()


if __name__ == "__main__":

    print(gcloud_reader().list_files())
