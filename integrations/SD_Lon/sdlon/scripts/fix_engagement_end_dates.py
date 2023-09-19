from datetime import datetime, timezone
import pickle
from typing import cast
from uuid import UUID

import click
import structlog
from gql import gql
from pydantic.networks import AnyHttpUrl
from raclients.graph.client import GraphQLClient
from sdclient.responses import Employment

from sdlon.config import get_changed_at_settings
from sdlon.date_utils import format_date, SD_INFINITY
from sdlon.fix_departments import FixDepartments
from sdlon.graphql import get_mo_client
from sdlon.scripts.fix_status8 import terminate_engagement
from sdlon.scripts.log import setup_logging

logger = structlog.get_logger(__name__)


def update_engagement(
    gql_client: GraphQLClient,
    eng_uuid: str,
    eng_from: str,
    eng_to: str | None,
    org_unit: str | None = None,
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

    payload = {
        "uuid": eng_uuid,
        "validity": {
            "from": eng_from,
            "to": eng_to,
        },
    }

    if org_unit is not None:
        payload["org_unit"] = org_unit

    gql_client.execute(
        mutation,
        variable_values={"input": payload},
    )


def unit_exists(gql_client: GraphQLClient, org_unit_uuid: UUID) -> bool:
    query = gql(
        """
            query GetOrgUnit($uuid: [UUID!]!) {
                org_units(uuids: $uuid) {
                    objects {
                        current {
                            uuid
                        }
                    }
                }
            }
        """
    )

    r = gql_client.execute(query, variable_values={"uuid": str(org_unit_uuid)})

    if not r["org_units"]["objects"]:
        return False
    return True


def fix_unit(fix_departments: FixDepartments, org_unit_uuid: UUID) -> None:
    today = datetime.today().date()
    fix_departments.fix_department(str(org_unit_uuid), today)


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
@click.option("--log-level", "log_level", default="INFO")
def main(
    auth_server: str,
    client_id: str,
    client_secret: str,
    mo_base_url: str,
    dry_run: bool,
    cpr: str,
    log_level: str,
):
    setup_logging(log_level)

    changed_at_settings = get_changed_at_settings()
    fix_departments = FixDepartments(changed_at_settings)

    gql_client = get_mo_client(
        cast(AnyHttpUrl, auth_server), client_id, client_secret, mo_base_url, 13
    )

    with open("/tmp/sdlon/diffs.bin", "rb") as fp:
        diffs = pickle.load(fp)

    if cpr:
        diffs = {k: v for k, v in diffs.items() if k[0] == cpr}

    failed_entities = {}
    processed_entities = {}
    non_processed_entities = {}
    units_ok = []
    for k, v in diffs.items():
        try:
            sd: Employment = v["sd"]
            sd_raw: Employment = v["sd_raw"]
            mo_eng = v["mo"]
            mismatches = v["mismatches"]

            # We have an active employment in SD, but not in MO
            if mo_eng is None:
                logger.warn("We do not handle cases where MO is None")
                continue

            if sd is None:
                # We have to use this date since the engagement cannot be found in SD
                to_date = format_date(datetime.now().date())
                logger.info(
                    "Terminate engagement",
                    uuid=mo_eng["uuid"],
                    user_key=mo_eng["user_key"],
                    to_date=to_date,
                )
                if not dry_run:
                    terminate_engagement(
                        gql_client,
                        mo_eng["uuid"],
                        to_date,
                    )
                continue

            # Skip all SD status 3 (orlov) engagements
            if sd is not None and sd.EmploymentStatus.EmploymentStatusCode in [
                "0",
                "3",
            ]:
                continue

            eng_uuid = mo_eng["uuid"]
            mo_end_date_iso = mo_eng["validity"]["to"]
            mo_end_date: str | None = (
                format_date(
                    datetime.fromisoformat(mo_end_date_iso)
                    .astimezone(timezone.utc)
                    .date()
                )
                if mo_end_date_iso is not None
                else None
            )
            sd_from_date = format_date(sd.EmploymentStatus.ActivationDate)
            sd_end_date: str | None = format_date(sd.EmploymentStatus.DeactivationDate)
            if sd_end_date == SD_INFINITY:
                sd_end_date = None

            if "Unit" in mismatches:
                units_to_check = [
                    sd_raw.EmploymentDepartment.DepartmentUUIDIdentifier,
                    sd.EmploymentDepartment.DepartmentUUIDIdentifier,
                ]
                for unit in units_to_check:
                    if unit not in units_ok:
                        unit_exists_in_mo = unit_exists(gql_client, unit)
                        if not unit_exists_in_mo:
                            fix_unit(fix_departments, unit)
                        units_ok.append(unit)
                logger.info(
                    "Update engagement org unit",
                    uuid=eng_uuid,
                    user_key=mo_eng["user_key"],
                    from_date=sd_from_date,
                    to_date=sd_end_date,
                    org_unit=str(sd.EmploymentDepartment.DepartmentUUIDIdentifier),
                )
                if not dry_run:
                    update_engagement(
                        gql_client,
                        eng_uuid,
                        sd_from_date,
                        sd_end_date,
                        str(sd.EmploymentDepartment.DepartmentUUIDIdentifier),
                    )
                processed_entities[k] = v

            if "End date" in mismatches:
                if sd_end_date is not None and mo_eng["validity"]["to"] is None:
                    logger.info(
                        "Terminate engagement",
                        uuid=eng_uuid,
                        user_key=mo_eng["user_key"],
                        to_date=sd_end_date,
                    )
                    if not dry_run:
                        terminate_engagement(gql_client, eng_uuid, sd_end_date)
                    if "Unit" not in mismatches:
                        processed_entities[k] = v
                    continue

                if not sd_end_date == mo_end_date:
                    logger.info(
                        "Update engagement end date",
                        uuid=eng_uuid,
                        user_key=mo_eng["user_key"],
                        from_date=sd_from_date,
                        to_date=sd_end_date,
                    )
                    if not dry_run:
                        update_engagement(
                            gql_client, eng_uuid, sd_from_date, sd_end_date
                        )
                    if "Unit" not in mismatches:
                        processed_entities[k] = v
                    continue

            logger.warn("Entity not processed", key=k, value=v)
            non_processed_entities[k] = v

        except Exception as err:
            logger.error("Entity failed", key=k, value=v, err=err)
            failed_entities[k] = v

    with open("/tmp/sdlon/failed_diffs.bin", "wb") as fp:
        pickle.dump(failed_entities, fp)

    with open("/tmp/sdlon/non_processed_diffs.bin", "wb") as fp:
        pickle.dump(non_processed_entities, fp)

    with open("/tmp/sdlon/processed_diffs.bin", "wb") as fp:
        pickle.dump(processed_entities, fp)


if __name__ == "__main__":
    main()
