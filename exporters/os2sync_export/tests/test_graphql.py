from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

from os2sync_export.os2mo import get_sts_user
from os2sync_export.os2mo import group_accounts
from tests.helpers import dummy_settings

mo_uuid = str(uuid4())
engagement_uuid1 = str(uuid4())
engagement_uuid2 = str(uuid4())
fk_org_uuid_1 = str(uuid4())
fk_org_user_key_1 = "AndersA"
fk_org_uuid_2 = str(uuid4())
fk_org_user_key_2 = ("AAnd",)
fk_org_uuid_3 = str(uuid4())

query_response = [
    {
        "uuid": str(uuid4()),
        "user_key": fk_org_uuid_2,
        "engagement_uuid": engagement_uuid2,
        "itsystem": {"name": "FK-ORG UUID"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": fk_org_user_key_1,
        "engagement_uuid": engagement_uuid1,
        "itsystem": {"name": "FK-ORG USERNAME"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": fk_org_uuid_3,
        "engagement_uuid": None,
        "itsystem": {"name": "FK-ORG UUID"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": fk_org_uuid_1,
        "engagement_uuid": engagement_uuid1,
        "itsystem": {"name": "FK-ORG UUID"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": fk_org_user_key_2,
        "engagement_uuid": engagement_uuid2,
        "itsystem": {"name": "FK-ORG USERNAME"},
    },
]


def test_group_by_engagement():
    groups = group_accounts(query_response, ["FK-ORG UUID"], "FK-ORG USERNAME")
    assert len(groups) == 3
    assert any(engagement_uuid1 == g.get("engagement_uuid") for g in groups)


@patch("os2sync_export.os2mo.get_sts_user_raw")
def test_get_sts_user(get_sts_user_raw_mock):
    gql_mock = MagicMock()
    gql_mock.execute.return_value = {
        "employees": [{"objects": [{"itusers": query_response}]}]
    }
    settings = dummy_settings
    settings.os2sync_uuid_from_it_systems = ["FK-ORG UUID"]
    settings.os2sync_user_key_it_system_name = "FK-ORG USERNAME"
    get_sts_user(mo_uuid=mo_uuid, gql_session=gql_mock, settings=settings)

    assert len(get_sts_user_raw_mock.call_args_list) == 3
    for c in [
        call(mo_uuid, settings, fk_org_uuid=fk_org_uuid_1, user_key=fk_org_user_key_1),
        call(mo_uuid, settings, fk_org_uuid=fk_org_uuid_2, user_key=fk_org_user_key_2),
        call(mo_uuid, settings, fk_org_uuid=fk_org_uuid_3, user_key=None),
    ]:
        assert c in get_sts_user_raw_mock.call_args_list
