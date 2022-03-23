from integrations.os2sync.os2mo import get_fk_org_uuid
from unittest.mock import patch, MagicMock
from integrations.os2sync.tests.helpers import MockOs2moGet
from parameterized import parameterized
from uuid import uuid4

class TestUUIDFromITSystem():
    mo_uuid = uuid4()
    fk_org_uuid = uuid4()
    other_it_system = uuid4()
    
    @parameterized.expand([
        ([],mo_uuid),
        ([{"uuid": fk_org_uuid, "itsystem": {"name": 'FK-org uuid'}}],fk_org_uuid),
        ([{"uuid": other_it_system, "itsystem": {"name": 'AD-account'}}], mo_uuid),
        ([{"uuid": other_it_system, "itsystem": {"name": 'AD-account'}}, {"uuid": fk_org_uuid, "itsystem": {"name": 'FK-org uuid'}}], fk_org_uuid),
    ])
    @patch("integrations.os2sync.os2mo.os2mo_get")
    def test_get_uuid(self, it_systems, expected, os2mo_get_mock):
        # os2mo_get_mock = MockOs2moGet(return_value=)
        with patch(
            "integrations.os2sync.os2mo.os2mo_get", return_value=MockOs2moGet(it_systems)
        ):
            
            uuid = get_fk_org_uuid("ou", self.mo_uuid)
            assert uuid == expected
        