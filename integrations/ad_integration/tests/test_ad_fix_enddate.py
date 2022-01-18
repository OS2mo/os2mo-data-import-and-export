from unittest.mock import patch
from uuid import uuid4

from ..ad_fix_enddate import CompareEndDate
from ..ad_fix_enddate import UpdateEndDate

# setup test variables and settings:
enddate_field = "enddate_field"
uuid_field = "uuid_field"
testcpr_field = "cpr_field"
testuuid = str(uuid4())
test_enddate = "2020-01-01"
test_search_base = "search_base"
test_settings = {
    "global": {"mora.base": "moramock"},
    "primary": {
        "cpr_field": testcpr_field,
        "search_base": test_search_base,
        "system_user": "username",
        "password": "password",
    },
}


@patch(
    "integrations.ad_integration.ad_common.AD._run_ps_script",
    return_value=[{enddate_field: "9999-12-31", uuid_field: testuuid, "CN": "TestCN"}],
)
@patch(
    "os2mo_helpers.mora_helpers.MoraHelper.read_user_engagement",
    return_value=[
        {"validity": {"from": "2019-01-01", "to": "2020-01-01"}, "is_primary": True}
    ],
)
@patch("integrations.ad_integration.ad_common.AD._create_session")
def test_ad_enddate_fixer(
    mock_ps_script,
    mock_read_user_engagements,
    mock_session,
):
    """Tests that user with no enddate in AD, but enddate in MO
    returns a ps-command to update AD.
    """
    c = CompareEndDate(enddate_field, uuid_field, test_settings)
    users = c.compare_mo()
    read_ps = f"""
        $User = "username"
        $PWord = ConvertTo-SecureString –String "password" –AsPlainText -Force
        $TypeName = "System.Management.Automation.PSCredential"
        $UserCredential = New-Object –TypeName $TypeName –ArgumentList $User, $PWord
        Get-ADUser -Filter \'{enddate_field} -like "9999-*"\' -SearchBase "{test_search_base}"  -Credential $usercredential -Properties cn,{uuid_field},{enddate_field} | ConvertTo-Json"""
    c._run_ps_script.assert_called_with(read_ps)
    assert list(users.keys()) == [testuuid]
    assert users[testuuid]["cn"] == "TestCN"

    u = UpdateEndDate(enddate_field, uuid_field, c.cpr_field, settings=test_settings)

    for uuid, end_date in u.get_changes(users):
        cmd = u.get_update_cmd(uuid, end_date)
        assert (
            cmd
            == f"""
        Get-ADUser  -SearchBase "{test_search_base}"  -Credential $usercredential -Filter \'{uuid_field} -eq "{testuuid}"\' |
        Set-ADUser  -Credential $usercredential -Replace @{{{enddate_field}="{test_enddate}"}} |
        ConvertTo-Json
        """
        )
