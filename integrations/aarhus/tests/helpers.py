import asyncio
from importlib import import_module
from typing import Optional
from typing import Tuple
from unittest import mock
from uuid import UUID

import config
import initial
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
        los_files = import_los_files()
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


def mock_config(**kwargs):
    class MockConfig:
        mox_base = "unused"

    instance = MockConfig()
    for name, val in kwargs.items():
        setattr(instance, name, val)

    return mock.patch.object(config, "get_config", return_value=instance)


def mock_create_mox_helper():
    return mock.patch.object(
        initial, "create_mox_helper", return_value=mock.AsyncMock()
    )


def _import_with_config(module_name):
    with mock_config(import_csv_folder="unused-path"):
        return import_module(module_name)


def import_los_files():
    return _import_with_config("los_files")


def import_los_leder():
    return _import_with_config("los_leder")
