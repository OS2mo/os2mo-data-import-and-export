import csv
from asyncio import gather
from functools import partial
from typing import Any
from typing import AsyncIterator
from uuid import UUID

import click
from fastapi.encoders import jsonable_encoder
from gql import gql
from gql.client import AsyncClientSession
from more_itertools import chunked
from more_itertools import one
from more_itertools import only
from pydantic import Extra
from pydantic import root_validator
from ra_utils.async_to_sync import async_to_sync
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient
from strawberry.dataloader import DataLoader


SETTINGS_ALIAS_MAP = {
    "exports_viborg_eksterne_outfile_basename": "filename",
    "exporters_viborg_primary_manager_responsibility": "manager_responsibility",
    "exporters_plan2learn_allowed_engagement_types": "engagement_type_blacklist",
}


class Settings(JobSettings):  # type: ignore
    class Config:
        extra = Extra.ignore
        frozen = True

    # Medarbejder (månedsløn) and Medarbejder (timeløn)
    engagement_type_blacklist: list[UUID] = []
    manager_responsibility: UUID
    filename: str = "viborg_ekstern.csv"

    # TODO: lc_choose_public_address code
    # settings.get("exports_viborg_eksterne.email.priority", []),

    @root_validator(pre=True)
    def aliases(cls, values):
        for key, new in SETTINGS_ALIAS_MAP.items():
            if key in values:
                values[new] = values[key]
        return values


async def load_managers(
    session: AsyncClientSession, responsibility: UUID, keys: list[UUID]
) -> list[tuple[str, str]]:
    query = gql(
        """
    query ViborgEksterneManagers($uuids: [UUID!], $responsibility: UUID!) {
      org_units(filter: {uuids: $uuids}) {
        objects {
          current {
            managers(
              filter: {responsibility: {uuids: [$responsibility]}}
            ) {
              person {
                name
                addresses(
                  filter: {address_type: {scope: "EMAIL"}}
                ) {
                  value
                }
              }
            }
          }
          uuid
        }
      }
    }
    """
    )
    result_map = {}

    org_unit_uuids = set(keys)
    for uuids in chunked(org_unit_uuids, 100):
        results = await session.execute(
            query,
            variable_values=jsonable_encoder(
                {
                    "uuids": uuids,
                    "responsibility": responsibility,
                }
            ),
        )
        for result in results["org_units"]["objects"]:
            org_unit_uuid = UUID(result["uuid"])
            manager = only(result["current"]["managers"])
            if manager is None:
                continue
            person = one(manager["person"])
            address = only(person["addresses"])
            if address is None:
                continue
            result_map[org_unit_uuid] = (person["name"], address["value"])

    return [result_map.get(key, ("", "")) for key in keys]


async def fetch_engagements(
    session: AsyncClientSession,
) -> AsyncIterator[dict[str, Any]]:
    query = gql(
        """
    query ViborgEksterneEngagements($cursor: Cursor) {
      page: engagements(
        cursor: $cursor
        limit: 100,
        filter: {from_date: null, to_date: null},
      ) {
        objects {
          objects {
            engagement_type {
              uuid
              name
            }
            user_key
            validity {
              from
            }
            org_unit {
              uuid
              name
            }
            person {
              name
              cpr_number
            }
          }
        }
        page_info {
          next_cursor
        }
      }
    }
    """
    )
    cursor = None
    while True:
        result = await session.execute(query, {"cursor": cursor})
        objects = result["page"]["objects"]
        for validities in objects:
            for obj in validities["objects"]:
                yield obj
        cursor = result["page"]["page_info"]["next_cursor"]
        if cursor is None:
            return


def obj2row(obj: dict[str, Any]) -> dict[str, Any]:
    engagement_type = obj["engagement_type"]
    org_unit = one(obj["org_unit"])
    person = one(obj["person"])
    return {
        "Enhedsnr": "Enhedsnr",
        "Enhedstype": "Enhedstype",
        "OrganisationsenhedUUID": UUID(org_unit["uuid"]),
        "Organisationsenhed": org_unit["name"],
        "CPR-nummer": person["cpr_number"],
        "Navn": person["name"],
        "Engagementstype": engagement_type["name"],
        "Tjenestenummer": obj["user_key"],
        "Startdato": obj["validity"]["from"],
    }


@click.command()
@async_to_sync
async def main():

    settings = Settings()

    def engagement_type_allowed(obj: dict[str, Any]) -> bool:
        return (
            UUID(obj["engagement_type"]["uuid"])
            not in settings.engagement_type_blacklist
        )

    gql_client = GraphQLClient(
        url=f"{settings.mora_base}/graphql/v20",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_realm=settings.auth_realm,
        auth_server=settings.auth_server,
        httpx_client_kwargs={"timeout": 300},
        execute_timeout=300,
    )
    rows = []
    async with gql_client as session:
        manager_loader = DataLoader(
            load_fn=partial(load_managers, session, settings.manager_responsibility)
        )

        async def add_manager(row: dict[str, Any]) -> dict[str, Any]:
            name, email = await manager_loader.load(row["OrganisationsenhedUUID"])
            return {
                **row,
                "Ledernavn": name,
                "Lederemail": email,
            }

        rows = [
            obj2row(obj)
            async for obj in fetch_engagements(session)
            if engagement_type_allowed(obj)
        ]
        # Add managers for the relevant org-units
        rows = list(await gather(*map(add_manager, rows)))

    fieldnames = [
        "OrganisationsenhedUUID",
        "Organisationsenhed",
        "Enhedsnr",
        "Enhedstype",
        "Ledernavn",
        "Lederemail",
        "Tjenestenummer",
        "CPR-nummer",
        "Navn",
        "Engagementstype",
        "Startdato",
    ]
    with open(settings.filename, encoding="utf-8", mode="w") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=fieldnames,
            extrasaction="ignore",
            delimiter=";",
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    main()
