from datetime import datetime
from operator import itemgetter

import los_files
import pytest
import util
from aiohttp import ClientResponseError
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


@pytest.mark.asyncio
async def test_terminate_details_handles_404_response(aioresponses):
    async def _run_test(ignored_http_statuses):
        mora_base = "http://example.com:8080"

        # Mock a 404 response from MO "terminate" API
        aioresponses.post(
            f"{mora_base}/service/details/terminate?force=1",
            status=404,
        )

        # Run `terminate_details`
        with mock_config(
            mora_base=mora_base,
            max_concurrent_requests=1,
            os2mo_chunk_size=1,
        ):
            async with util.get_client_session() as client_session:
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
