from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

from os2sync_export.os2mo import get_sts_user
from os2sync_export.os2mo import group_accounts
from tests.helpers import dummy_settings

engagement_uuid1 = str(uuid4())
engagement_uuid2 = str(uuid4())
query_response = [
    {
        "uuid": str(uuid4()),
        "user_key": "AndersA",
        "engagement": [{"uuid": engagement_uuid1}],
        "itsystem": {"name": "FK-ORG USERNAME"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": str(uuid4()),
        "engagement": [{"uuid": engagement_uuid1}],
        "itsystem": {"name": "FK-ORG UUID"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": "AAnd",
        "engagement": [{"uuid": engagement_uuid2}],
        "itsystem": {"name": "FK-ORG USERNAME"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": str(uuid4()),
        "engagement": [{"uuid": engagement_uuid2}],
        "itsystem": {"name": "FK-ORG UUID"},
    },
    {
        "uuid": str(uuid4()),
        "user_key": str(uuid4()),
        "engagement": None,
        "itsystem": {"name": "FK-ORG UUID"},
    },
]


def test_group_by_engagement():
    groups = group_accounts(query_response, ["FK-ORG UUID"], "FK-ORG USERNAME")
    assert len(groups) == 3
    assert any(engagement_uuid1 == g.get("engagement_uuid") for g in groups)


@patch("os2sync_export.os2mo.get_sts_user_raw")
def test_get_sts_user(get_sts_user_raw_mock):
    mo_uuid = str(uuid4())
    gql_mock = MagicMock()
    gql_mock.execute.return_value = {
        "employees": [{"objects": [{"itusers": query_response}]}]
    }
    settings = dummy_settings
    settings.os2sync_uuid_from_it_systems = ["FK-ORG UUID"]
    settings.os2sync_user_key_it_system_name = "FK-ORG USERNAME"
    get_sts_user(mo_uuid=mo_uuid, gql_session=gql_mock, settings=dummy_settings)
    assert len(get_sts_user_raw_mock.call_args_list) == 3
