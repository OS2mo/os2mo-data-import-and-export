# Fix end dates for weird engagements which terminates in
# (approximately) 9999-12-29
from datetime import datetime
from typing import Any

import click
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
from more_itertools import one

from reports.graphql import get_mo_client

GET_ALL_ENGAGEMENTS = gql(
    """
    query GetAllEngagements {
      engagements(filter: { to_date: null }) {
        objects {
          uuid
          validities {
            user_key
            validity {
              from
              to
            }
          }
        }
      }
    }
    """
)

GET_ENGAGEMENT = gql(
    """
    query GetEngagement($uuid: UUID!) {
      engagements(
        filter: { uuids: [$uuid], to_date: null }
      ) {
        objects {
          uuid
          validities {
            validity {
              from
              to
            }
            employee_uuid
            engagement_type_uuid
            extension_1
            extension_10
            extension_2
            extension_3
            extension_4
            extension_5
            extension_6
            extension_7
            extension_8
            extension_9
            fraction
            job_function_uuid
            org_unit_uuid
            primary_uuid
            user_key
          }
        }
      }
    }
    """
)

UPDATE_ENGAGEMENT = gql(
    """
    mutation UpdateEngagement($input:EngagementUpdateInput!) {
      engagement_update(input: $input) {
        uuid
      }
    }
    """
)


def get_extension(validity: dict[str, Any], n: int) -> str:
    value = validity[f"extension_{n}"]
    # Due to MO weirdness we have to convert None to an empty string
    # in order to avoid breaking things in the DB
    return value if value is not None else ""


def fix_engagement(gql_client: GraphQLClient, eng_uuid: str) -> None:
    print("Fixing engagement", eng_uuid)

    engagement = gql_client.execute(GET_ENGAGEMENT, variable_values={"uuid": eng_uuid})
    obj = one(engagement["engagements"]["objects"])

    validity = one(
        validity
        for validity in obj["validities"]
        if datetime.fromisoformat(validity["validity"]["from"]).year != 9999
        and validity["validity"]["to"] is None
    )

    gql_client.execute(
        UPDATE_ENGAGEMENT,
        variable_values={
            "input": {
                "uuid": eng_uuid,
                "validity": {
                    "from": validity["validity"]["from"],
                    "to": validity["validity"]["to"],
                },
                "engagement_type": validity["engagement_type_uuid"],
                "extension_1": get_extension(validity, 1),
                "extension_2": get_extension(validity, 2),
                "extension_3": get_extension(validity, 3),
                "extension_4": get_extension(validity, 4),
                "extension_5": get_extension(validity, 5),
                "extension_6": get_extension(validity, 6),
                "extension_7": get_extension(validity, 7),
                "extension_8": get_extension(validity, 8),
                "extension_9": get_extension(validity, 9),
                "extension_10": get_extension(validity, 10),
                "fraction": validity["fraction"]
                if validity["fraction"] is not None
                else 0,
                "job_function": validity["job_function_uuid"],
                "org_unit": validity["org_unit_uuid"],
                "person": validity["employee_uuid"],
                "primary": validity.get("primary_uuid"),
                "user_key": validity["user_key"],
            }
        },
    )

    print("Engagement updated", eng_uuid)


@click.command()
@click.option("--client-secret", required=True)
def main(client_secret: str) -> None:
    print("Script started")

    gql_client = get_mo_client(
        auth_server="http://localhost:8090/auth",  # type: ignore
        client_id="developer",
        client_secret=client_secret,
        mo_base_url="http://localhost:5000",
        gql_version=25,
    )

    print("Getting all engagements... stay cool for a MOment")
    all_engagements = gql_client.execute(GET_ALL_ENGAGEMENTS)
    engagements_to_fix = [
        obj
        for obj in all_engagements["engagements"]["objects"]
        if any(
            year == 9999
            for year in [
                datetime.fromisoformat(validity["validity"]["from"]).year
                for validity in obj["validities"]
            ]
        )
    ]
    print(f"Number of engagements to fix: {len(engagements_to_fix)}")

    for obj in engagements_to_fix:
        fix_engagement(gql_client, obj["uuid"])

    print("Done!")


if __name__ == "__main__":
    main()
