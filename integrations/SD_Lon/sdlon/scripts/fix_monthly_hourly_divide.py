# Update all engagements with the correct value of "Timelønnet" or
# "Månedslønnet" according to the value of the environment variable
# SD_MONTHLY_HOURLY_DIVIDE. If the SD EmploymentIdentifier (user_key of the
# employment) is above the value set in this ENV, they should be "Timelønnet"
# or else they should be "Månedslønnet"
from datetime import date
from enum import Enum
from uuid import UUID

import click
from gql import gql
from more_itertools import one
from pydantic import BaseModel
from raclients.graph.client import GraphQLClient

from sdlon.date_utils import format_date, parse_datetime
from sdlon.graphql import get_mo_client


class SalaryType(Enum):
    HOURLY = "timeløn"
    MONTHLY = "månedsløn"


class Engagement(BaseModel):
    eng_uuid: UUID
    user_key: str
    eng_type_uuid: UUID
    from_date: date


def get_eng_type_uuid(
        gql_client: GraphQLClient, salary_type: SalaryType
) -> UUID:
    """
    Get the UUID of the "Timelønnet" engagement type
    """
    query = gql(
        """
            query GetEngagementTypeUUID($user_key: [String!]!) {
              classes(user_keys: $user_key) {
                objects {
                  user_key
                  type
                  name
                  uuid
                }
              }
            }
        """
    )

    r = gql_client.execute(query, variable_values={"user_key": salary_type.value})

    return UUID(one(r["classes"]["objects"])["uuid"])


def get_engagement_user_key_and_type(
        gql_client: GraphQLClient
) -> list[Engagement]:
    """
    Get all engagements (user_keys and engagement_types)

    Args:
        gql_client: The GraphQL client

    Returns:
        List of Engagements
    """

    query = gql(
        """
            query GetEngagementAndType {
              engagements {
                objects {
                  objects {
                    uuid
                    user_key
                    engagement_type {
                      uuid
                      user_key
                      name
                    }
                    validity {
                      from
                    }
                  }
                }
              }
            }
        """
    )

    r = gql_client.execute(query)
    objects = r["engagements"]["objects"]

    return [
        Engagement(
            eng_uuid=UUID(one(obj["objects"])["uuid"]),
            user_key=one(obj["objects"])["user_key"],
            eng_type_uuid=UUID(one(obj["objects"])["engagement_type"]["uuid"]),
            from_date=parse_datetime(one(obj["objects"])["validity"]["from"][:10]).date()
        )
        for obj in objects
    ]


def update_engagement_type(
        gql_client: GraphQLClient, eng_uuid: UUID, eng_type_uuid: UUID, from_date: date
) -> None:
    """
    Update the engagement type of an engagement from a given date.

    Args:
        eng_uuid: The engagement UUID
        eng_type_uuid: The UUID of the engagement type
        from_date: The date from which the change should take effect
    """

    mutation = gql(
        """
            mutation UpdateEngagement($input: EngagementUpdateInput!) {
                engagement_update(input: $input) {
                    uuid
                }
            }
        """
    )

    gql_client.execute(mutation, variable_values={
        "input": {
            "uuid": str(eng_uuid),
            "validity": {"from": format_date(from_date)},
            "engagement_type": str(eng_type_uuid)
        }
    })


@click.command()
@click.option(
    "--auth-server",
    "auth_server",
    type=click.STRING,
    default="http://localhost:8090/auth",
    help="Keycloak auth server URL"
)
@click.option(
    "--client-id",
    "client_id",
    type=click.STRING,
    default="dipex",
    help="Keycloak client id"
)
@click.option(
    "--client-secret",
    "client_secret",
    type=click.STRING,
    required=True,
    help="Keycloak client secret for the DIPEX client"
)
@click.option(
    "--mo-base-url",
    "mo_base_url",
    type=click.STRING,
    default="http://localhost:5000",
    help="Base URL for calling MO"
)
@click.option(
    "--monthly-hourly-divide",
    type=click.INT,
    required=True,
    help="The new SD_MONTHLY_HOURLY_DIVIDE"
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    help="Do not perform any changes in MO"
)
def main(
        auth_server: str,
        client_id: str,
        client_secret: str,
        mo_base_url: str,
        monthly_hourly_divide: int,
        dry_run: bool
):
    gql_client = get_mo_client(
        auth_server, client_id, client_secret, mo_base_url, 5
    )

    hourly_eng_type_uuid = get_eng_type_uuid(gql_client, SalaryType.HOURLY)
    monthly_eng_type_uuid = get_eng_type_uuid(gql_client, SalaryType.MONTHLY)

    engagements = get_engagement_user_key_and_type(gql_client)

    hourly_engagements_to_fix = [
        eng for eng in engagements
        if eng.eng_type_uuid == hourly_eng_type_uuid and
           int(eng.user_key) < monthly_hourly_divide
    ]
    print("Number of hourly engagements to fix:", len(hourly_engagements_to_fix))
    for eng in hourly_engagements_to_fix:
        if not dry_run:
            update_engagement_type(
                gql_client, eng.eng_uuid, monthly_eng_type_uuid, eng.from_date)
        print(f"Updated engagement {eng.eng_uuid} from hourly to monthly from {format_date(eng.from_date)}")

    monthly_engagements_to_fix = [
        eng for eng in engagements
        if eng.eng_type_uuid == monthly_eng_type_uuid and
           int(eng.user_key) >= monthly_hourly_divide
    ]
    print("Number of monthly engagements to fix:", len(monthly_engagements_to_fix))
    for eng in monthly_engagements_to_fix:
        if not dry_run:
            update_engagement_type(
                gql_client, eng.eng_uuid, hourly_eng_type_uuid, eng.from_date)
        print(f"Updated engagement {eng.eng_uuid} from monthly to hourly from {format_date(eng.from_date)}")


if __name__ == "__main__":
    main()
