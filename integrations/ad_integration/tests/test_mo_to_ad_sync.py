from unittest import mock
from unittest import TestCase
from uuid import uuid4

from ..mo_to_ad_sync import run_mo_to_ad_sync
from ..mo_to_ad_sync import run_preview_command_for_uuid
from .mocks import MockADParameterReader
from .test_utils import dict_modifier
from .test_utils import TestADWriterMixin


@mock.patch("time.sleep")
class TestMoToAdSync(TestCase, TestADWriterMixin):
    """Test `mo_to_ad_sync`"""

    def setUp(self):
        super().setUp()

        settings_transformer = dict_modifier(
            {
                "integrations.ad_writer.template_to_ad_fields": {
                    "Name": "{{ mo_values['full_name'] }} - testing"
                },
            }
        )

        def remove_manager_cpr(mo_values, *args, **kwargs):
            del mo_values["manager_cpr"]
            return mo_values

        self._setup_adwriter(
            early_transform_settings=settings_transformer,
            transform_mo_values=remove_manager_cpr,
        )
        self._mock_reader = MockADParameterReader()

    def test_all_ad_users(self, *args):
        self._assert_stats_ok(self._run())

    def test_single_ad_user(self, *args):
        self._assert_stats_ok(self._run(sync_username="not important"))

    def test_single_ad_user_can_be_renamed(self, *args):
        self._run(sync_username="not important")
        rename_cmd = self.ad_writer.scripts[0].splitlines()[-1]
        self.assertIn("-NewName", rename_cmd)

    def test_invalid_uuid_field_does_nothing(self, *args):
        self._assert_stats_ok(
            self._run(mo_uuid_field="invalid field name"),
            num_attempted=0,
            num_successful=0,
        )

    def test_unhandled_exception(self, *args):
        with mock.patch.object(self.ad_writer, "sync_user", side_effect=Exception):
            with self.assertLogs("export") as cm:
                self._assert_stats_ok(
                    self._run(),
                    num_successful=0,
                    num_critical_error=1,
                )
                self.assertEqual(len(cm.records), 1)
                self.assertRegex(
                    cm.records[0].message, r"Error updating AD user '.*?': .*"
                )

    def test_preview_command_for_uuid(self, *args):
        with mock.patch("click.echo_via_pager") as mock_echo:
            commands = run_preview_command_for_uuid(
                self._mock_reader,
                self.ad_writer,
                uuid4(),  # MO user UUID
                sync_cpr="cpr",
            )
            self.assertEqual(mock_echo.call_count, 3)
            self.assertEqual(len(commands), 3)
            for cmd in commands:
                self.assertIsInstance(cmd, str)

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
