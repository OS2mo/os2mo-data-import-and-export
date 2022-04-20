import asyncio
from types import ModuleType
from typing import Optional
from typing import Tuple
from unittest import mock
from uuid import UUID

import config
import initial
import los_files
import util

from integrations.dar_helper import dar_helper


class HelperMixin:
    def _run_until_complete(self, coro):
        future = asyncio.ensure_future(coro)
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(future)

    def _mock_dar_lookup(self, return_value: Tuple[Optional[str], Optional[UUID]]):
        return mock.patch.object(
            dar_helper,
            "dar_datavask_multiple",
            return_value=[return_value],
        )

    def _mock_read_csv(self, instance):
        return mock.patch.object(los_files, "read_csv", return_value=[instance])

    def _mock_util_call(self, name, return_value=None):
        return mock.patch.object(
            util, name, return_value=return_value or mock.MagicMock()
        )

    def _mock_get_client_session(self):
        return self._mock_util_call("get_client_session")

    def _mock_create_details(self):
        return self._mock_util_call("create_details")

    def _mock_edit_details(self):
        return self._mock_util_call("edit_details")

    def _mock_terminate_details(self):
        return self._mock_util_call("terminate_details")

    def _mock_lookup_employees(self, return_value=None):
        return self._mock_util_call("lookup_employees", return_value=return_value)

    def _mock_lookup_organisationfunktion(self, return_value=None):
        return self._mock_util_call(
            "lookup_organisationfunktion", return_value=return_value
        )

    def _mock_get_fileset_implementation(self, fileset):
        return mock.patch.object(
            los_files, "get_fileset_implementation", return_value=fileset
        )

    def _mock_get_import_filenames(self, filenames):
        fileset = mock.Mock(spec=los_files.FileSet)
        fileset.get_import_filenames = mock.Mock(return_value=filenames)
        return self._mock_get_fileset_implementation(fileset)

    def _mock_get_modified_datetime(self, return_value=None, side_effect=None):
        fileset = mock.Mock(spec=los_files.FileSet)
        fileset.get_modified_datetime = mock.Mock(
            return_value=return_value, side_effect=side_effect
        )
        return self._mock_get_fileset_implementation(fileset)

    def _mock_settings_json(self, settings=None):
        return mock.patch("config.load_settings", return_value=settings)


def mock_config(**kwargs):
    class MockConfig:
        mox_base = "unused"

    instance = MockConfig()
    for name, val in kwargs.items():
        setattr(instance, name, val)

    return mock.patch.object(config, "get_config", return_value=instance)


def mock_create_mox_helper(module: ModuleType = initial):
    mock_mox_helper = mock.AsyncMock()
    mock_mox_helper.return_value = mock.AsyncMock()
    return mock.patch.object(module, "create_mox_helper", return_value=mock_mox_helper)
