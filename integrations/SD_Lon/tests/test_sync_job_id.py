from unittest import TestCase

from integrations.SD_Lon.config import ChangedAtSettings
from integrations.SD_Lon.sync_job_id import JobIdSync


class JobIdSyncTest(JobIdSync):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _read_classes(self):
        self.engagement_types = []
        self.job_function_types = []


class Test_sync_job_id(TestCase):
    def setUp(self):
        settings = ChangedAtSettings(
            mora_base="http://dummy.url",
            sd_job_function="JobPositionIdentifier",
            sd_import_run_db="run_db.sqlite",
            sd_institution_identifier="XY",
            sd_monthly_hourly_divide=9000,
            sd_password="secret",
            sd_user="user",
        )
        self.job_id_sync = JobIdSyncTest(settings)

    def test_create(self):
        self.assertTrue(self.job_id_sync.update_job_functions)
