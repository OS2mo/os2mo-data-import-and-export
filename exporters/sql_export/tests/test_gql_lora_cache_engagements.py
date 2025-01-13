from unittest.mock import AsyncMock
from uuid import uuid4

from fastramqpi.ra_utils.async_to_sync import async_to_sync
from graphql import ExecutionResult

from ..gql_lora_cache_async import GQLLoraCache


class MockGqlLoraCache(GQLLoraCache):
    async def _get_org_uuid(self) -> str:
        return str(uuid4())


gql_response = {
    "page": {
        "objects": [
            {
                "uuid": "00e96933-91e4-42ac-9881-0fe1738b2e59",
                "obj": [
                    {
                        "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                        "engagement_type_uuid": "8acc5743-044b-4c82-9bb9-4e572d82b524",
                        "extension_1": None,
                        "extension_10": None,
                        "extension_2": None,
                        "extension_3": None,
                        "extension_4": None,
                        "extension_5": None,
                        "extension_6": None,
                        "extension_7": None,
                        "extension_8": None,
                        "extension_9": None,
                        "fraction": None,
                        "is_primary": True,
                        "job_function_uuid": "cf7adae4-22a6-4973-819f-73957eaec265",
                        "org_unit_uuid": "1c690f27-35c5-5c02-975a-930e6b524805",
                        "primary_uuid": "0644cd06-b84b-42e0-95fe-ce131c21fbe6",
                        "user_key": "-",
                        "uuid": "00e96933-91e4-42ac-9881-0fe1738b2e59",
                        "validity": {"from": "2000-06-29T00:00:00+02:00", "to": None},
                    }
                ],
            }
        ],
        "page_info": {"next_cursor": None},
    }
}
expected = {
    "00e96933-91e4-42ac-9881-0fe1738b2e59": [
        {
            "fraction": None,
            "user_key": "-",
            "uuid": "00e96933-91e4-42ac-9881-0fe1738b2e59",
            "extensions": {
                "udvidelse_1": None,
                "udvidelse_2": None,
                "udvidelse_3": None,
                "udvidelse_4": None,
                "udvidelse_5": None,
                "udvidelse_6": None,
                "udvidelse_7": None,
                "udvidelse_8": None,
                "udvidelse_9": None,
                "udvidelse_10": None,
            },
            "from_date": "2000-06-29",
            "to_date": "9999-12-31",
            "user": "b81b5097-90b7-4991-8752-c860e1e59fd3",
            "engagement_type": "8acc5743-044b-4c82-9bb9-4e572d82b524",
            "job_function": "cf7adae4-22a6-4973-819f-73957eaec265",
            "unit": "1c690f27-35c5-5c02-975a-930e6b524805",
            "primary_type": "0644cd06-b84b-42e0-95fe-ce131c21fbe6",
            "primary_boolean": True,
        }
    ]
}


@async_to_sync
async def test_cache_engagements():
    lc = GQLLoraCache(full_history=True)
    lc.gql_client_session = AsyncMock()
    lc.gql_client_session.return_value.execute.side_effect = [
        ExecutionResult(data=gql_response, extensions={"__page_out_of_range": True})
    ]
    await lc._cache_lora_engagements()
    lc.gql_client_session.return_value.execute.assert_awaited_once()
    assert lc.engagements == expected
