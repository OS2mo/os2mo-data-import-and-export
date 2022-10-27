import logging
from unittest.mock import patch

from .. import IntegrationSettings
from .. import LogLevel


_LOG_FILENAME: str = "/tmp/foo.log"


class _ExampleSettings(IntegrationSettings):
    log_filename = _LOG_FILENAME


class _ExamplePrefixSettings(_ExampleSettings):
    class Config:
        settings_json_prefix: str = "a."


def test_log_level_uses_default():
    with patch("shared.load_settings", return_value={}):
        settings: _ExampleSettings = _ExampleSettings()
        assert settings.log_level == LogLevel.ERROR.value  # the default is ERROR


def test_json_settings_source_handles_file_not_found():
    with patch("shared.load_settings", side_effect=FileNotFoundError):
        settings: _ExampleSettings = _ExampleSettings()
        assert settings.log_level == LogLevel.ERROR.value  # the default is ERROR


def test_log_level_uses_settings_json_value():
    with patch("shared.load_settings", return_value={"log_level": "INFO"}):
        settings: _ExampleSettings = _ExampleSettings()
        assert settings.log_level == LogLevel.INFO.value


def test_json_settings_source_filters_on_prefix():
    with patch("shared.load_settings", return_value={"a.a": "a", "b.b": "b"}):
        settings: _ExamplePrefixSettings = _ExamplePrefixSettings()
        assert settings.a_a == "a"
        assert "b_b" not in settings.dict()


def test_start_logging_based_on_settings():
    # This test checks the actual log file contents, rather than using the pytest
    # `caplog` fixture. This is because `start_logging_based_on_settings` calls
    # `logging.basicConfig` which sets up the logging machinery without `caplog` being
    # aware of this. Since we want to test `start_logging_based_on_settings`, we make
    # assertions on its expected behavior, i.e. writing to the specified log file.

    # Arrange
    logger = logging.getLogger(__name__)
    with patch("shared.load_settings", return_value={}):
        settings: _ExampleSettings = _ExampleSettings()
        # Act: set up logging and make logging calls
        settings.start_logging_based_on_settings()
        logger.info("info")
        logger.error("error")

    # Assert: only ERROR log lines are present, since the default log level is ERROR
    with open(_LOG_FILENAME, "r") as log:
        log_lines = log.readlines()
        assert {"info" not in line for line in log_lines}
        assert {"error" in line for line in log_lines}

    # Cleanup: truncate log file so it is empty at the next test run
    with open(_LOG_FILENAME, "w") as log:
        log.write("")
