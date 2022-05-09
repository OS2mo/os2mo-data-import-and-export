from uuid import uuid4

from parameterized import parameterized

from integrations.os2sync.os2mo import get_fk_org_uuid


FK_ORG_UUID_IT_SYSTEM = "FK-org uuid"
AD_OBJECTGUID_IT_SYSTEM = "AD ObjectGuid"


class TestUUIDFromITSystem:
    mo_uuid = uuid4()
    fk_org_uuid = uuid4()
    ad_uuid = uuid4()

    @parameterized.expand(
        [
            # Paramerers are:
            # settings, it-accounts, expected chosen uuid
            # With no list in settings and no it-accounts, choose mo_uuid
            ([], [], mo_uuid),
            # With no list in settings, choose mo_uuid whatever it-accounts exist
            (
                [],
                [{"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}}],
                mo_uuid,
            ),
            (
                [],
                [{"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}}],
                mo_uuid,
            ),
            (
                [],
                [
                    {"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}},
                    {"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}},
                ],
                mo_uuid,
            ),
            # With one it-system in settings we use the uuid from that it-system or from MO if there are no it-account in that it-system.
            # No it-accounts - use mo_uuid
            ([FK_ORG_UUID_IT_SYSTEM], [], mo_uuid),
            # There is an it-account, it is in settings - chose the value from that
            (
                [FK_ORG_UUID_IT_SYSTEM],
                [{"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}}],
                fk_org_uuid,
            ),
            # There is an it-account, it is not in settings - chose mo_uuid.
            (
                [FK_ORG_UUID_IT_SYSTEM],
                [{"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}}],
                mo_uuid,
            ),
            # There are two it-accounts - chose the value from the correct one.
            (
                [FK_ORG_UUID_IT_SYSTEM],
                [
                    {"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}},
                    {"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}},
                ],
                fk_org_uuid,
            ),
            # With two it-systems in settings we use the uuid from the first, if it exists, then second, else from MO.
            # There are no it-accounts, use mo_uuid
            ([FK_ORG_UUID_IT_SYSTEM, AD_OBJECTGUID_IT_SYSTEM], [], mo_uuid),
            # There is one account, it is in settings, use the uuid from that.
            (
                [FK_ORG_UUID_IT_SYSTEM, AD_OBJECTGUID_IT_SYSTEM],
                [{"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}}],
                fk_org_uuid,
            ),
            # There is one account, it is in settings, but second in the list, use the uuid from that it account.
            (
                [FK_ORG_UUID_IT_SYSTEM, AD_OBJECTGUID_IT_SYSTEM],
                [{"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}}],
                ad_uuid,
            ),
            # There are two it-accounts, both it-systems are in settings, use the value from the first in the settings list.
            (
                [FK_ORG_UUID_IT_SYSTEM, AD_OBJECTGUID_IT_SYSTEM],
                [
                    {"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}},
                    {"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}},
                ],
                fk_org_uuid,
            ),
            # Same as above, but with settings list reversed to test we use the value from the first in the settings list.
            (
                [AD_OBJECTGUID_IT_SYSTEM, FK_ORG_UUID_IT_SYSTEM],
                [
                    {"uuid": ad_uuid, "itsystem": {"name": AD_OBJECTGUID_IT_SYSTEM}},
                    {"uuid": fk_org_uuid, "itsystem": {"name": FK_ORG_UUID_IT_SYSTEM}},
                ],
                ad_uuid,
            ),
        ]
    )
    def test_get_uuid(self, prioritized_list, it_systems, expected):
        # os2mo_get_mock = MockOs2moGet(return_value=)
        uuid = get_fk_org_uuid(
            it_systems, self.mo_uuid, uuid_from_it_systems=prioritized_list
        )
        assert uuid == expected
