from unittest import mock
from uuid import uuid4

import config
import los_import
from click.testing import CliRunner


class TestMain:
    def test_settings_exclude_empty_command_line_args(self):
        runner = CliRunner()
        with mock.patch.object(config.Settings, "from_kwargs") as mock_from_kwargs:
            # Run `main` as Click would
            runner.invoke(los_import.main)
            # Assert no (empty) Click options were passed to
            # `config.Settings.from_kwargs`.
            mock_from_kwargs.assert_called_once_with()

    def test_settings_include_command_line_args(self):
        runner = CliRunner()
        it_system_uuid = uuid4()  # Random valid UUID
        with mock.patch.object(config.Settings, "from_kwargs") as mock_from_kwargs:
            # Run `main` as Click would
            runner.invoke(
                los_import.main,
                ["--azid-it-system-uuid=%s" % it_system_uuid],
            )
            # Assert config was built, and that command line arg was used
            mock_from_kwargs.assert_called_once()
            kwargs = mock_from_kwargs.call_args.kwargs
            assert kwargs["azid_it_system_uuid"] == it_system_uuid
