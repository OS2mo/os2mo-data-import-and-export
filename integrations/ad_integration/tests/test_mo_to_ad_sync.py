from unittest import mock
from unittest import TestCase

from ..mo_to_ad_sync import run_mo_to_ad_sync
from .test_utils import TestADWriterMixin


class _MockADReader(TestADWriterMixin):
    def read_user(self, **kwargs):
        return self._prepare_get_from_ad(ad_transformer=None)

    def read_it_all(self, **kwargs):
        return [self.read_user()]


class TestMoToAdSync(TestCase, TestADWriterMixin):
    """Test `mo_to_ad_sync`"""

    def setUp(self):
        super().setUp()

        def remove_manager_cpr(mo_values, *args, **kwargs):
            del mo_values["manager_cpr"]
            return mo_values

        self._setup_adwriter(transform_mo_values=remove_manager_cpr)
        self.ad_writer._wait_for_replication = lambda: None
        self._mock_reader = _MockADReader()

    def test_all_ad_users(self):
        self._assert_stats_ok(self._run())

    def test_single_ad_user(self):
        self._assert_stats_ok(self._run(sync_username="not important"))

    def test_invalid_uuid_field_does_nothing(self):
        self._assert_stats_ok(
            self._run(mo_uuid_field="invalid field name"),
            num_attempted=0,
            num_successful=0,
        )

    def test_unhandled_exception(self):
        with mock.patch.object(self.ad_writer, "sync_user", side_effect=Exception):
            self._assert_stats_ok(
                self._run(),
                num_successful=0,
                num_critical_error=1,
            )

    def _run(self, mo_uuid_field="ObjectGUID", **kwargs):
        return run_mo_to_ad_sync(
            self._mock_reader,
            self.ad_writer,
            mo_uuid_field=mo_uuid_field,
            **kwargs,
        )

    def _assert_stats_ok(
        self,
        stats,
        num_attempted=1,
        num_successful=1,
        num_critical_error=0,
    ):
        self.assertDictEqual(
            stats,
            {
                "attempted_users": num_attempted,
                "updated": num_successful,
                "fully_synced": num_successful,
                "nothing_to_edit": 0,
                "no_manager": 0,
                "unknown_manager_failure": 0,
                "cpr_not_unique": 0,
                "user_not_in_mo": 0,
                "user_not_in_ad": 0,
                "critical_error": num_critical_error,
                "unknown_failed_sync": 0,
                "no_active_engagement": 0,
            },
        )
