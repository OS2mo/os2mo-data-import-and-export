from unittest import mock
from unittest import TestCase

from parameterized import parameterized

from ..ad_writer import ADWriter
from ..mo_to_ad_sync import run_mo_to_ad_sync
from ..mo_to_ad_sync import run_preview_command_for_uuid
from .mocks import MO_UUID
from .mocks import MockADParameterReader
from .mocks import MockADParameterReaderWithManager
from .mocks import MockADWriterContext
from .mocks import MockLoraCacheExtended
from .mocks import MockLoraCacheWithManager
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


class TestMoToAdSyncDryRun:
    @parameterized.expand(
        [
            # 1. Engagement in the present, dry run should count it as an update
            (
                # `lc` contains (default) primary engagement in the present
                MockLoraCacheExtended(mo_values={"uuid": MO_UUID}),
                # Assert that the dry run would count this MO user as one to process
                lambda stats: stats["attempted_users"] == stats["updated"] == 1,
            ),
            # 2. No engagements, dry run should count it as a skipped user
            (
                # `lc` contains no engagements
                MockLoraCacheExtended(mo_values={"uuid": MO_UUID}, mo_engagements=[]),
                # Assert that the dry run would count this MO user as skipped, due to
                # having no active primary engagements.
                lambda stats: stats["attempted_users"] == stats["nothing_to_edit"] == 1,
            ),
        ]
    )
    def test_dry_run_counts_skipped_when_no_engagements_in_loracache(
        self, lc, test_stats
    ):
        # Arrange
        reader = MockADParameterReader()
        with MockADWriterContext() as mock_ad_writer_context:
            writer = ADWriter(lc=lc, lc_historic=lc)
            writer.sync_user = mock.MagicMock()
            # Act
            stats = run_mo_to_ad_sync(reader, writer, "ObjectGUID", dry_run=True)
            # Assert stats are as expected
            assert test_stats(stats)
            # Assert that no actual Powershell commands were issued
            writer.sync_user.assert_not_called()
            mock_ad_writer_context.mock_session.run_ps.assert_not_called()


class TestMoToAdSyncPreview:
    def test_preview_command_for_uuid(self, *args):
        # Arrange
        reader = MockADParameterReaderWithManager()
        lc = MockLoraCacheWithManager()
        with MockADWriterContext():
            writer = ADWriter(lc=lc, lc_historic=lc)
            with mock.patch("click.echo_via_pager") as mock_echo:
                # Act
                commands = run_preview_command_for_uuid(
                    reader,
                    writer,
                    MO_UUID,
                    sync_username="user_sam",
                )
                # Assert
                assert mock_echo.call_count == 4
                assert len(commands) == 4
                for cmd in commands:
                    assert isinstance(cmd, str)
