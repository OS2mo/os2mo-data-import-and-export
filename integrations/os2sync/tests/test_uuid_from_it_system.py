from integrations.os2sync.os2mo import get_fk_org_uuid
from unittest.mock import patch, MagicMock
from integrations.os2sync.tests.helpers import MockOs2moGet
from parameterized import parameterized
from uuid import uuid4
from constants import AD_objectguid_it_system, FK_org_uuid_it_system

class TestUUIDFromITSystem():
    mo_uuid = uuid4()
    fk_org_uuid = uuid4()
    ad_uuid = uuid4()
    
    @parameterized.expand([
        # With no list in settings we allways use mo_uuid
        ([], [], mo_uuid),
        ([], [{"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}],mo_uuid),
        ([], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}], mo_uuid),
        ([], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}, {"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}], mo_uuid),
        # With one it-system in settings we use the uuid from that it-system or from MO.
        ([FK_org_uuid_it_system], [],mo_uuid),
        ([FK_org_uuid_it_system], [{"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}],fk_org_uuid),
        ([FK_org_uuid_it_system], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}], mo_uuid),
        ([FK_org_uuid_it_system], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}, {"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}], fk_org_uuid),
        # With two it-systems in settings we use the uuid from the first, if it exists, then second, else from MO.
        ([FK_org_uuid_it_system, AD_objectguid_it_system], [], mo_uuid),
        ([FK_org_uuid_it_system, AD_objectguid_it_system], [{"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}],fk_org_uuid),
        ([FK_org_uuid_it_system, AD_objectguid_it_system], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}], ad_uuid),
        ([FK_org_uuid_it_system, AD_objectguid_it_system], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}, {"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}], fk_org_uuid),
        ([AD_objectguid_it_system, FK_org_uuid_it_system], [{"uuid": ad_uuid, "itsystem": {"name": AD_objectguid_it_system}}, {"uuid": fk_org_uuid, "itsystem": {"name": FK_org_uuid_it_system}}], ad_uuid),
    ])
    def test_get_uuid(self, prioritized_list, it_systems, expected):
        # os2mo_get_mock = MockOs2moGet(return_value=)
        uuid = get_fk_org_uuid(it_systems, self.mo_uuid, uuid_from_it_systems=prioritized_list)
        assert uuid == expected
        