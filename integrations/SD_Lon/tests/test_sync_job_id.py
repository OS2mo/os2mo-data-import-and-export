from unittest import TestCase
from unittest.mock import MagicMock

from integrations.SD_Lon.sync_job_id import JobIdSync


class JobIdSyncTest(JobIdSync):
    def __init__(self, *args, **kwargs):
        self.morahelper_mock = MagicMock()

        super().__init__(*args, **kwargs)

    def _get_mora_helper(self, mora_base):
        return self.morahelper_mock


class Test_sync_job_id(TestCase):

    def setUp(self):
        self.job_id_sync = JobIdSyncTest({
            'mora.base': 'dummy_url',
            'integrations.SD_Lon.job_function': 'JobPositionIdentifier',
        })

    def test_create(self):
        self.assertTrue(self.job_id_sync.update_job_functions)
