import pytest
from fastramqpi.depends import LegacyGraphQLSession

from ...gql_lora_cache_async import GQLLoraCache


@pytest.mark.integration_test
async def test_read_engagements(
    legacy_graphql_session: LegacyGraphQLSession,
) -> None:
    """Equivalence test which uses the two versions and checks that the result is the same"""

    gql_cache = GQLLoraCache(graphql_session=legacy_graphql_session)
    engagements_old = await gql_cache._fetch_engagements()

    # The settings object is immutable - this seems to be the easiest way to switch the flag
    settings = gql_cache.settings.dict()
    settings["use_new_cache"] = True

    gql_cache = GQLLoraCache(
        graphql_session=legacy_graphql_session,
        settings=settings,
    )
    engagements_new = await gql_cache._fetch_engagements()

    assert engagements_old == engagements_new
