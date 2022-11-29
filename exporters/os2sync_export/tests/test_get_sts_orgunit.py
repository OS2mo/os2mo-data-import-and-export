from unittest.mock import patch
from uuid import uuid4

import pytest
from os2sync_export.os2mo import get_sts_orgunit
from tests.helpers import dummy_settings

top_level_uuid = str(uuid4())
other_top_level_uuid = str(uuid4())


class MockUnitWParent:
    def json(self):
        return {"uuid": uuid4(), "parent": {"uuid": other_top_level_uuid}}


class TestGetOrgUnit:
    @patch("os2sync_export.os2mo.os2mo_get", return_value=MockUnitWParent())
    @patch("os2sync_export.os2mo.is_ignored", return_value=False)
    def test_get_sts_orgunit(self, mock_os2mo_get, ignored_mock):
        with pytest.raises(ValueError):
            get_sts_orgunit("test", settings=dummy_settings)
