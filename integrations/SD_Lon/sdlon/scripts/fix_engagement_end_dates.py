import pickle
from typing import cast

import click
import structlog
from gql import gql
from pydantic.networks import AnyHttpUrl
from raclients.graph.client import GraphQLClient
from sdclient.responses import Employment

from sdlon.date_utils import format_date, SD_INFINITY
from sdlon.graphql import get_mo_client
from sdlon.scripts.fix_status8 import terminate_engagement

logger = structlog.get_logger(__name__)


def update_engagement_end_date(
    gql_client: GraphQLClient, eng_uuid: str, eng_from: str, eng_to: str | None
) -> None:
    mutation = gql(
        """
            mutation UpdateEngagement($input: EngagementUpdateInput!) {
                engagement_update(input: $input) {
                    uuid
                }
            }
        """
    )

    gql_client.execute(
        mutation,
        variable_values={
            "input": {
                "uuid": eng_uuid,
                "validity": {
                    "from": eng_from,
                    "to": eng_to,
                },
            }
        },
    )


@click.command()
@click.option(
    "--auth-server",
    "auth_server",
    default="http://keycloak:8080/auth",
    help="Keycloak auth server URL",
)
@click.option("--client-id", "client_id", default="sdlon", help="Keycloak client id")
@click.option(
    "--client-secret",
    "client_secret",
    envvar="CLIENT_SECRET",
    required=True,
    help="Keycloak client secret for the DIPEX client",
)
@click.option(
    "--mo-base-url",
    "mo_base_url",
    type=click.STRING,
    default="http://mo:5000",
    help="Base URL for calling MO",
)
@click.option(
    "--dry-run", "dry_run", is_flag=True, help="Do not perform any changes in MO"
)
@click.option("--cpr", "cpr", help="Only make the comparison for this CPR")
def main(
    auth_server: str,
    client_id: str,
    client_secret: str,
    mo_base_url: str,
    dry_run: bool,
    cpr: str,
):
    gql_client = get_mo_client(
        cast(AnyHttpUrl, auth_server), client_id, client_secret, mo_base_url, 13
    )

    with open("/tmp/diffs.bin", "rb") as fp:
        diffs = pickle.load(fp)

    if cpr:
        diffs = {k: v for k, v in diffs.items() if k[0] == cpr}

    for k, v in diffs.items():
        try:
            sd: Employment = v["sd"]
            mo_eng = v["mo"]
            mismatches = v["mismatches"]

            eng_uuid = mo_eng["uuid"]
            sd_from_date = format_date(sd.EmploymentStatus.ActivationDate)
            sd_end_date = format_date(sd.EmploymentStatus.DeactivationDate)
            if sd_end_date == SD_INFINITY:
                continue

            if "End date" in mismatches:
                logger.info(
                    "Update engagement",
                    uuid=eng_uuid,
                    user_key=mo_eng["user_key"],
                    from_date=sd_from_date,
                    to_date=sd_end_date,
                )
                if not dry_run:
                    terminate_engagement(gql_client, eng_uuid, sd_end_date)
                    # update_engagement_end_date(
                    #     gql_client, eng_uuid, sd_from_date, sd_end_date
                    # )
        except Exception:
            print(k)
            print(v)
            exit(1)


if __name__ == "__main__":
    main()
