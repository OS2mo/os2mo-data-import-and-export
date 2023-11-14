from ra_utils.async_to_sync import async_to_sync

from ..gql_lora_cache_async import GQLLoraCache


@async_to_sync
async def test_simple_query():
    lc = GQLLoraCache()
    gql_obj, variable_values = await lc.construct_query(
        query_fields="uuid",
        query_type="engagements",
        variable_values={},
        simple_query=True,
    )
    expected = """
            query ($limit: int, $offset: int) {
                page: engagements(limit: $limit, offset: $offset){
                    uuid
                }
            }

            """
    assert gql_obj == expected
    assert variable_values == {}


@async_to_sync
async def test_actual_state_query():
    lc = GQLLoraCache()
    gql_obj, variable_values = await lc.construct_query(
        query_fields="uuid",
        query_type="engagements",
        variable_values={},
        simple_query=False,
    )
    expected = """
            query ($limit: int, $offset: int) {
                page: engagements(limit: $limit, offset: $offset){
                    uuid
                    obj: current {
                        uuid
                    }
                }
            }

            """
    assert gql_obj == expected
    assert variable_values == {}


@async_to_sync
async def test_historic_query():
    lc = GQLLoraCache()
    lc.full_history = True
    gql_obj, variable_values = await lc.construct_query(
        query_fields="uuid",
        query_type="engagements",
        variable_values={},
        simple_query=False,
    )
    expected = """
            query ($limit: int, $offset: int, $to_date: DateTime, $from_date: DateTime) {
                page: engagements(limit: $limit, offset: $offset, from_date: $from_date, to_date: $to_date){
                    uuid
                    obj: objects {
                        uuid
                    }
                }
            }

            """
    assert gql_obj == expected
    assert variable_values == {"from_date": None, "to_date": None}
