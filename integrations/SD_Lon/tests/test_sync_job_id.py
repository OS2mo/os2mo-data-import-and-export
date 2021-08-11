from unittest import TestCase

from integrations.SD_Lon.sync_job_id import JobIdSync


class JobIdSyncTest(JobIdSync):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _read_classes(self):
        self.engagement_types = []
        self.job_function_types = []


class Test_sync_job_id(TestCase):
    def setUp(self):
        self.job_id_sync = JobIdSyncTest(
            {
                "mora.base": "dummy_url",
                "integrations.SD_Lon.job_function": "JobPositionIdentifier",
            }
        )

    def test_create(self):
        self.assertTrue(self.job_id_sync.update_job_functions)
