from unittest.mock import patch
from uuid import uuid4

import pytest

from integrations.os2sync.config import get_os2sync_settings
from integrations.os2sync.os2mo import get_sts_orgunit

top_level_uuid = str(uuid4())
other_top_level_uuid = str(uuid4())


class MockUnitWParent:
    def json(self):
        return {"uuid": uuid4(), "parent": {"uuid": other_top_level_uuid}}


class TestGetOrgUnit:
    testsettings = get_os2sync_settings(
        os2sync_top_unit_uuid=top_level_uuid, municipality=1
    )

    @patch("integrations.os2sync.os2mo.os2mo_get", return_value=MockUnitWParent())
    @patch("integrations.os2sync.os2mo.is_ignored", return_value=False)
    def test_get_sts_orgunit(self, mock_os2mo_get, ignored_mock):
        with pytest.raises(ValueError):
            get_sts_orgunit("test", settings=self.testsettings)
