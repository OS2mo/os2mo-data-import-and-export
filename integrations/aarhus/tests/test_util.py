import contextlib
import typing
from datetime import datetime
from operator import itemgetter
from typing import Optional
from unittest.mock import MagicMock
from unittest.mock import patch

import aiohttp.client
import los_files
import pytest
import pytest_aioresponses
import util
from aiohttp import ClientResponseError
from aiohttp import ClientSession
from hypothesis import given
from hypothesis import strategies as st
from more_itertools import one

from .helpers import mock_config


class TestParseFilenames:
    def test_parses_prefix_correctly(self):
        filenames = [
            "xxxxOrg_nye_20210128_123742.csv",
            "Org_nye_20210128_123742.csv",
            "garbagebagasdasdads",
        ]

        expected_filename = "Org_nye_20210128_123742.csv"

        actual = los_files.parse_filenames(filenames, "Org_nye", datetime.min)

        actual_filename, _ = one(actual)

        assert expected_filename == actual_filename

    def test_filters_dates_correctly(self):
        filenames = [
            "Org_nye_20200128_123742.csv",
            "Org_nye_20201028_123742.csv",
            "Org_nye_20210128_123742.csv",
        ]

        expected = [
            ("Org_nye_20201028_123742.csv", datetime(2020, 10, 28, 12, 37, 42)),
            ("Org_nye_20210128_123742.csv", datetime(2021, 1, 28, 12, 37, 42)),
        ]

        actual = los_files.parse_filenames(
            filenames, "Org_nye", datetime(2020, 6, 1, 0, 0, 0)
        )

        assert expected == actual

    @given(
        st.permutations(
            [
                "Org_nye_20210101_000000.csv",
                "Org_nye_20200101_000000.csv",
                "Org_nye_20190101_000000.csv",
                "Org_nye_20180101_000000.csv",
            ]
        )
    )
    def test_sorts_output(self, filenames):
        expected_dates = [
            datetime(2018, 1, 1),
            datetime(2019, 1, 1),
            datetime(2020, 1, 1),
            datetime(2021, 1, 1),
        ]

        actual = los_files.parse_filenames(filenames, "Org_nye", datetime.min)
        actual_dates = list(map(itemgetter(1), actual))

        assert expected_dates == actual_dates


@contextlib.asynccontextmanager
async def _mock_mo_response(
    aioresponses: pytest_aioresponses,
    mo_endpoint: str,
    mo_http_status: int,
    mo_response: Optional[dict] = None,
) -> typing.AsyncGenerator[ClientSession, None]:
    mora_base = "http://example.com:8080"
    # Mock a response from MO
    aioresponses.post(
        f"{mora_base}{mo_endpoint}?force=1",
        status=mo_http_status,
        payload=mo_response or {},
    )
    # Create client session on mock base URL
    with mock_config(
        mora_base=mora_base,
        max_concurrent_requests=1,
        os2mo_chunk_size=1,
    ):
        async with util.get_client_session() as session:
            yield session


@pytest.mark.asyncio
async def test_terminate_details_handles_404_response(
    aioresponses: pytest_aioresponses,
):
    async def _run_test(ignored_http_statuses):
        # Mock a 404 response from MO "terminate" API, and run `terminate_details`
        async with _mock_mo_response(
            aioresponses, "/service/details/terminate", 404
        ) as client_session:
            detail_payloads = [{"foo": "bar"}]
            await util.terminate_details(
                client_session,
                detail_payloads,
                ignored_http_statuses=ignored_http_statuses,
            )

    # Expect no exceptions if 404 is ignored
    await _run_test(ignored_http_statuses=(404,))

    # Expect an exception if `raise_for_status` is called in `submit_payloads`
    with pytest.raises(ClientResponseError):
        await _run_test(ignored_http_statuses=None)


@pytest.mark.asyncio
async def test_unhandled_status_400(aioresponses: pytest_aioresponses):
    async with _mock_mo_response(
        aioresponses, "/service/details/create", 400, {}
    ) as client_session:
        with pytest.raises(ClientResponseError):
            await util.create_details(client_session, [{"foo": "bar"}])


@pytest.mark.asyncio
async def test_handled_status_400(aioresponses: pytest_aioresponses):
    async with _mock_mo_response(
        aioresponses,
        "/service/details/create",
        400,
        {"error_key": "V_DUPLICATED_IT_USER"},
    ) as client_session:
        await util.create_details(client_session, [{"foo": "bar"}])


@pytest.mark.asyncio
@patch("aiohttp.client.ClientSession.post")
async def test_connection_error(
    mock_post: MagicMock, aioresponses: pytest_aioresponses
):
    mock_post.side_effect = aiohttp.client.ServerDisconnectedError
    async with _mock_mo_response(
        aioresponses, "/service/details/create", 500, {}
    ) as client_session:
        with pytest.raises(aiohttp.ServerDisconnectedError):

            await util.create_details(client_session, [{"foo": "bar"}])

        assert mock_post.call_count == util.retry_attempts
