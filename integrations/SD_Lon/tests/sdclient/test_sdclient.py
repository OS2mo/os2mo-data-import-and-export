from datetime import date
from unittest import skip
from unittest.mock import patch

from sdlon.sdclient.client import SDClient
from sdlon.sdclient.requests import GetDepartmentRequest


@skip("Not finished")
@patch("sdlon.sdclient.client.httpx.get")
def test_get_department(mock_get):
    sd_client = SDClient("username", "password")

    query_params = GetDepartmentRequest(
        InstitutionIdentifier="XY",
        ActivationDate=date(2023, 1, 1),
        DeactivationDate=date(2023, 12, 31)
    )

    sd_client.call_sd(query_params)

    print(mock_get.calls)
