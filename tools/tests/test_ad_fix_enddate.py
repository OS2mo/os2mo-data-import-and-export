from unittest.mock import patch
from uuid import uuid4

from tools.data_fixers.ad_fix_enddate import CompareEndDate

enddate_field = "enddate_field"
uuid_field = "uuid_field"
testuuid = str(uuid4())
test_enddate = "2020-01-01"


@patch(
    "integrations.ad_integration.ad_common.AD._run_ps_script",
    return_value=[{enddate_field: "9999-12-31", uuid_field: testuuid, "CN": "TestCN"}],
)
@patch(
    "os2mo_helpers.mora_helpers.MoraHelper.read_user_engagement",
    return_value=[{"validity": {"from": "2019-01-01", "to": None}, "is_primary": True}],
)
def test_CompareEndDate(mock_ps_script, mock_read_user_engagements):

    c = CompareEndDate(enddate_field, uuid_field)
    users = c.compare_mo()
    c._run_ps_script.assert_called()
    assert users == {}
