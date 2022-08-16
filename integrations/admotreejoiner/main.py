import json
from collections import defaultdict
from datetime import date
from itertools import chain
from itertools import groupby
from itertools import starmap
from operator import itemgetter
from typing import Callable
from typing import Iterator
from typing import TypeVar
from uuid import UUID

from more_itertools import one
from gql import gql
from raclients.graph.client import GraphQLClient

import click
from app.config import Settings
from app.ldap import configure_ad_connection
from asciitree import LeftAligned
from ra_utils.async_to_sync import async_to_sync
from raclients.modelclient.mo import ModelClient
from ramodels.mo import OrganisationUnit


def load_ad_users(settings, ad_connection, guids) -> set[str]:
    guid_conditions = "".join(map(lambda guid: f"(objectGUID={guid})", guids))
    search_filter = "(&(objectclass=user)(|" + guid_conditions + "))"

    ad_connection.search(
        search_base=settings.ad_search_base,
        search_filter=search_filter,
        # Search in the entire subtree of search_base
        search_scope="SUBTREE",
        # TODO: We really should find orgunits via the distinguishedName?
        # attributes=["extensionAttribute4"],
        attributes=["distinguishedName"]
    )
    json_str = ad_connection.response_to_json()
    ad_response = json.loads(json_str)
    results = list(map(lambda result: result["attributes"]["distinguishedName"] or "", ad_response["entries"]))
    results = filter(lambda result: result != "", results)
    results = map(lambda result: ",".join(result.split(",")[1:]), results)
    results = set(results)
    # print(results)
    return results


async def fetch_org_unit_uuids(gql_session) -> set[UUID]:
    query = gql(
        """
        query OrgUnitUUIDs {
          org_units {
            uuid
          }
        }
        """
    )
    result = await gql_session.execute(query)
    org_unit_uuids = set(map(itemgetter("uuid"), result["org_units"]))
    return org_unit_uuids


async def fetch_org_engagements(gql_session, org_unit_uuid: UUID, itsystem_uuid: UUID) -> set[UUID]:
    query = gql(
        """
        query OrgUnitEngagements($uuids: [UUID!]) {
          org_units(uuids: $uuids) {
            uuid
            objects {
              engagements {
                employee {
                  uuid
                  itusers {
                    itsystem_uuid
                    uuid
                    user_key
                  }
                }
              }
            }
          }
        }
        """
    )
    result = await gql_session.execute(query, variable_values={"uuids": [str(org_unit_uuid)]})

    engagements = one(one(result["org_units"])["objects"])["engagements"]
    from more_itertools import flatten
    itusers = flatten(map(
        lambda engagement: one(engagement["employee"])["itusers"],
        engagements
    ))
    itusers = filter(
        lambda ituser: UUID(ituser["itsystem_uuid"]) == itsystem_uuid,
        itusers
    )
    adguids = set(map(itemgetter("user_key"), itusers))
    return adguids


@click.command()
@async_to_sync
async def upload_adtree():
    settings = Settings()

    import structlog
    import logging

    log_level_value = logging.getLevelName("INFO")
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level_value)
    )

    client_kwargs = dict(
        client_id=settings.fastramqpi.client_id,
        client_secret=settings.fastramqpi.client_secret.get_secret_value(),
        auth_realm=settings.fastramqpi.auth_realm,
        auth_server=settings.fastramqpi.auth_server,
    )

    gql_client = GraphQLClient(
        url=settings.fastramqpi.mo_url + "/graphql",
        execute_timeout=settings.fastramqpi.graphql_timeout,
        httpx_client_kwargs={"timeout": settings.fastramqpi.graphql_timeout},
        **client_kwargs,
    )

    from app.dataloaders import load_itsystems
    itsystem_uuid = settings.adguid_itsystem_uuid
    itsystem_user_key = settings.adguid_itsystem_user_key
    if itsystem_uuid is None:
        async with gql_client as gql_session:
            itsystem_uuids = await load_itsystems([itsystem_user_key], gql_session)
            itsystem_uuid = one(itsystem_uuids)
        if itsystem_uuid is None:
            message = "Unable to find itsystem by user-key"
            logger.warn(message, itsystem_user_key=itsystem_user_key)
            raise ValueError(message)

    from tqdm import tqdm

    ad_connection = configure_ad_connection(settings)

    no_data = 0
    unique = 0
    inconsistent = 0
    exception = 0

    with ad_connection:
        async with gql_client as gql_session:
            org_unit_uuids = await fetch_org_unit_uuids(gql_session)
            for org_unit_uuid in (pbar := tqdm(org_unit_uuids)):
                pbar.set_description(f"Processing {org_unit_uuid}")

                try:
                    # print(f"Processing {org_unit_uuid}")
                    adguids = await fetch_org_engagements(gql_session, org_unit_uuid, itsystem_uuid)
                    # print(adguids)
                    if len(adguids) == 0:
                        # print(f"No data to do mapping for: {org_unit_uuid}")
                        no_data += 1
                        continue

                    ad_orgunits = load_ad_users(settings, ad_connection, adguids)
                    if len(ad_orgunits) == 0:
                        # print(f"No data to do mapping for: {org_unit_uuid}")
                        no_data += 1
                        continue
                    if len(ad_orgunits) > 1:
                        # print(f"Inconsistent data to do mapping for: {org_unit_uuid}")
                        # print(f"Could be one of {ad_orgunits}")
                        inconsistent += 1
                        continue
                    # print("Unique mapping found!")
                    ad_org_unit = one(ad_orgunits)
                    # print(f"Mapping {org_unit_uuid} to {ad_org_unit}")
                    # TODO: Translate ad orgunit name to MO UUID
                    unique += 1
                except Exception:
                    exception += 1
                    continue

    print("no_data", no_data)
    print("unique", unique)
    print("inconsistent", inconsistent)
    print("exception", exception)

    return

    async with gql_client as gql_session:
        query = gql(
            """
            query OrgUnit(
              $user_keys: [String!]
            ) {
              org_units(user_keys: $user_keys) {
                objects {
                  uuid
                }
              }
            }
            """
        )
        result = await gql_session.execute(
            query, variable_values={"user_keys": [org_name]}
        )
        print(result)
        ad_engagement_target = one(one(result["org_units"])["objects"])["uuid"]
        print(ad_engagement_target)

    print("mapping", ad_engagement_target, "<-->", mo_engagement_target)


if __name__ == "__main__":
    upload_adtree()

