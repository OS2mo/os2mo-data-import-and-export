import datetime
from unittest.mock import Mock
from unittest.mock import patch

import pytest as pytest
from hypothesis import given
from hypothesis import strategies as st
from raclients.graph.client import GraphQLClient

from ..ad_fix_enddate import CompareEndDate
from ..ad_fix_enddate import UpdateEndDate
from .mocks import AD_UUID_FIELD
from .mocks import MO_UUID
from .mocks import MockADParameterReader

# setup test variables and settings:
enddate_field = "enddate_field"
uuid_field = "uuid_field"
test_search_base = "search_base"
test_settings = {
    "global": {"mora.base": "moramock"},
    "primary": {
        "search_base": test_search_base,
        "system_user": "username",
        "password": "password",
    },
}
mora_base = "http://mo"
client_id = "dipex"
client_secret = "603f1c82-d012-4d04-9382-dbe659c533fb"
auth_realm = "mo"
auth_server = "http://keycloak:8080/auth"
ad_null_date = datetime.date(9999, 12, 31)


@patch("integrations.ad_integration.ad_common.AD._create_session")
def get_c(mock_session):
    with GraphQLClient(
        url=f"{mora_base}/graphql/v3",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        return CompareEndDate(
            enddate_field=enddate_field,
            uuid_field=uuid_field,
            graph_ql_session=session,
            settings=test_settings,
        )


@given(date=st.datetimes())
def test_to_enddate(date):
    c = get_c()
    assert c.to_enddate(str(date)) == date.date()
    assert c.to_enddate(None) == ad_null_date
    assert c.to_enddate("9999-12-31") == ad_null_date


@pytest.mark.parametrize(
    "eng",
    [
        {
            "engagements": [
                {"objects": [{"validity": {"to": "2021-09-02T00:00:00+02:00"}}]},
                {"objects": [{"validity": {"to": "2022-09-02T00:00:00+02:00"}}]},
                {"objects": [{"validity": {"to": "2023-09-02T00:00:00+02:00"}}]},
            ]
        },
        {
            "engagements": [
                {
                    "objects": [
                        {"validity": {"to": "2021-09-02T00:00:00+02:00"}},
                        {"validity": {"to": "2022-09-02T00:00:00+02:00"}},
                        {"validity": {"to": "2023-09-02T00:00:00+02:00"}},
                    ]
                }
            ]
        },
    ],
)
def test_get_employee_end_date(eng):
    with patch(
        "gql.client.SyncClientSession.execute",
        return_value=eng,
    ):
        c = get_c()
        known_latest_date = datetime.date(2023, 9, 2)
        found_latest_date = c.get_employee_end_date(
            "e5e28d11-0513-4db8-8487-39d0b1102376"
        )
        print(found_latest_date)
        assert found_latest_date == known_latest_date


@patch("integrations.ad_integration.ad_common.AD._create_session")
@given(uuid=st.uuids(), enddate=st.dates())
def test_ad_enddate_cmd(
    mock_session,
    uuid,
    enddate,
):
    u = UpdateEndDate(
        enddate_field=enddate_field, uuid_field=uuid_field, settings=test_settings
    )

    cmd = u.get_update_cmd(uuid, enddate)
    assert (
        cmd
        == f"""
        Get-ADUser  -SearchBase "{test_search_base}"  -Credential $usercredential -Filter \'{uuid_field} -eq "{uuid}"\' |
        Set-ADUser  -Credential $usercredential -Replace @{{{enddate_field}="{enddate}"}} |
        ConvertTo-Json
        """
    )


class _TestableCompareEndDateNoMatchingADUser(CompareEndDate):
    def get_all_ad_users(self):
        return MockADParameterReader().read_it_all()


class _TestableCompareEndDateADUserHasMOUUID(_TestableCompareEndDateNoMatchingADUser):
    def get_all_ad_users(self):
        ad_users = super().get_all_ad_users()
        for ad_user in ad_users:
            ad_user[AD_UUID_FIELD] = MO_UUID
        return ad_users


class _TestableCompareEndDateADUserUpToDate(_TestableCompareEndDateADUserHasMOUUID):
    def get_all_ad_users(self):
        ad_users = super().get_all_ad_users()
        for ad_user in ad_users:
            ad_user[enddate_field] = "2022-12-31"
        return ad_users


@pytest.fixture()
def mock_graphql_session():
    graphql_session = Mock()
    graphql_session.execute = Mock()
    graphql_session.execute.return_value = {
        "engagements": [{"objects": [{"validity": {"to": "2022-12-31"}}]}]
    }
    return graphql_session


@pytest.fixture()
def mock_graphql_session_raising_keyerror():
    graphql_session = Mock()
    graphql_session.execute = Mock()
    graphql_session.execute.return_value = {}
    return graphql_session


@patch("integrations.ad_integration.ad_common.AD._create_session")
@pytest.mark.parametrize(
    "cls,expected_result",
    [
        # If no matching AD user, don't return a MO user UUID and MO end date
        (_TestableCompareEndDateNoMatchingADUser, {}),
        # If matching AD user exists *and* its AD end date is already up to date, don't
        # return a MO user UUID and MO end date.
        (_TestableCompareEndDateADUserUpToDate, {}),
        # If matching AD user exists *but* its AD end date is *not* up to date, return
        # the MO user UUID and MO end date.
        (_TestableCompareEndDateADUserHasMOUUID, {MO_UUID: "2022-12-31"}),
    ],
)
def test_get_end_dates_to_fix(
    mock_create_session, mock_graphql_session, cls, expected_result
):
    instance = cls(
        enddate_field, AD_UUID_FIELD, mock_graphql_session, settings=test_settings
    )
    actual_result = instance.get_end_dates_to_fix(MO_UUID)
    assert actual_result == expected_result


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_get_end_dates_to_fix_handles_keyerror(
    mock_create_session, mock_graphql_session_raising_keyerror
):
    instance = _TestableCompareEndDateADUserHasMOUUID(
        enddate_field,
        AD_UUID_FIELD,
        mock_graphql_session_raising_keyerror,
        settings=test_settings,
    )
    assert instance.get_end_dates_to_fix(MO_UUID) == {}
