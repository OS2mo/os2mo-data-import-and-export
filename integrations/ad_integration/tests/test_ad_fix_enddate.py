import datetime
from unittest.mock import Mock
from unittest.mock import patch

import pytest as pytest
from hypothesis import given
from hypothesis import HealthCheck
from hypothesis import settings
from hypothesis import strategies as st

from ..ad_fix_enddate import CompareEndDate
from ..ad_fix_enddate import MOEngagementDateSource
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


def _get_mock_graphql_session(return_value):
    graphql_session = Mock()
    graphql_session.execute = Mock()
    graphql_session.execute.return_value = return_value
    return graphql_session


@pytest.fixture()
def mock_graphql_session():
    return _get_mock_graphql_session(
        {"engagements": [{"objects": [{"validity": {"to": "2022-12-31"}}]}]}
    )


@pytest.fixture()
def mock_graphql_session_raising_keyerror():
    return _get_mock_graphql_session({})


@pytest.fixture()
def mock_mo_engagement_date_source(mock_graphql_session):
    return MOEngagementDateSource(mock_graphql_session, 0)


@pytest.fixture()
def mock_mo_engagement_date_source_raising_keyerror(
    mock_graphql_session_raising_keyerror,
):
    return MOEngagementDateSource(mock_graphql_session_raising_keyerror, 0)


@pytest.fixture()
def mock_compare_end_date(mock_mo_engagement_date_source: MOEngagementDateSource):
    with patch("integrations.ad_integration.ad_common.AD._create_session"):
        return _TestableCompareEndDateADUserHasMOUUID(
            enddate_field,
            uuid_field,
            mock_mo_engagement_date_source,
            settings=test_settings,
        )


@given(date=st.datetimes())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_to_enddate(date, mock_mo_engagement_date_source):
    assert mock_mo_engagement_date_source.to_enddate(str(date)) == date.date()
    assert mock_mo_engagement_date_source.to_enddate(None) == ad_null_date
    assert mock_mo_engagement_date_source.to_enddate("9999-12-31") == ad_null_date


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
    mo_engagement_date_source = MOEngagementDateSource(
        _get_mock_graphql_session(eng), 0
    )
    known_latest_date = datetime.date(2023, 9, 2)
    found_latest_date = mo_engagement_date_source.get_employee_end_date(
        MO_UUID,
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
    u = UpdateEndDate(enddate_field, uuid_field, settings=test_settings)
    cmd = u.get_update_cmd(uuid, enddate)
    assert (
        cmd
        == f"""
        Get-ADUser  -SearchBase "{test_search_base}"  -Credential $usercredential -Filter \'{uuid_field} -eq "{uuid}"\' |
        Set-ADUser  -Credential $usercredential -Replace @{{{enddate_field}="{enddate}"}} |
        ConvertTo-Json
        """
    )


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
    mock_create_session, mock_mo_engagement_date_source, cls, expected_result
):
    instance = cls(
        enddate_field,
        AD_UUID_FIELD,
        mock_mo_engagement_date_source,
        settings=test_settings,
    )
    actual_result = instance.get_end_dates_to_fix(MO_UUID)
    assert actual_result == expected_result


@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_get_end_dates_to_fix_handles_keyerror(
    mock_create_session,
    mock_mo_engagement_date_source_raising_keyerror,
):
    instance = _TestableCompareEndDateADUserHasMOUUID(
        enddate_field,
        AD_UUID_FIELD,
        mock_mo_engagement_date_source_raising_keyerror,
        settings=test_settings,
    )
    assert instance.get_end_dates_to_fix(MO_UUID) == {}
