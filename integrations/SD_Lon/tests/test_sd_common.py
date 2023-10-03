import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import patch

import pytest
from pytest import MonkeyPatch
from sdlon.config import CommonSettings
from sdlon.models import JobFunction
from sdlon.sd_common import read_employment_at
from sdlon.sd_common import sd_lookup


@pytest.fixture()
def common_settings() -> CommonSettings:
    return CommonSettings(
        sd_global_from_date=date(2000, 1, 1),
        sd_import_run_db="not used",
        sd_institution_identifier="dummy",
        sd_user="user",
        sd_password="password",
        sd_job_function=JobFunction.employment_name,
        sd_monthly_hourly_divide=1,
    )


@patch("sdlon.sd_common.sd_lookup")
def test_return_none_when_sd_employment_empty(
    mock_sd_lookup,
    common_settings: CommonSettings,
) -> None:
    mock_sd_lookup.return_value = OrderedDict()
    assert read_employment_at(date(2000, 1, 1), common_settings) is None


def test_sd_lookup_logs_payload_to_db(
    monkeypatch: MonkeyPatch,
    common_settings: CommonSettings,
) -> None:
    # Arrange
    test_request_uuid = uuid.uuid4()
    test_url: str = "test_url"
    test_params: dict[str, Any] = {"params": "mocked"}
    test_response: str = f"""<{test_url}><Foo bar="baz"></Foo></{test_url}>"""
    test_status_code = 200

    @dataclass
    class _MockResponse:
        text: str
        status_code: int

    def mock_requests_get(url: str, **kwargs: Any):
        return _MockResponse(text=test_response, status_code=test_status_code)

    def mock_log_payload(
        request_uuid: uuid.UUID,
        full_url: str,
        params: str,
        response: str,
        status_code: int,
    ):
        # Assert
        assert request_uuid == test_request_uuid
        assert full_url.endswith(test_url)
        assert params == str(test_params)
        assert response == test_response
        assert status_code == test_status_code

    monkeypatch.setattr("sdlon.sd_common.requests.get", mock_requests_get)
    monkeypatch.setattr("sdlon.sd_common.log_payload", mock_log_payload)

    # Act
    sd_lookup(test_url, common_settings, test_params, request_uuid=test_request_uuid)
