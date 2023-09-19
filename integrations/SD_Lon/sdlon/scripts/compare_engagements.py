import logging
import pathlib
import pickle
from copy import deepcopy
from datetime import datetime
from pprint import pprint
from typing import Any, cast
from uuid import UUID

import click
from gql import gql
from more_itertools import one
from pydantic.networks import AnyHttpUrl
from raclients.graph.client import GraphQLClient
from ramodels.mo.employee import Employee
import structlog

from sdclient.client import SDClient
from sdclient.requests import GetEmploymentRequest, GetOrganizationRequest
from sdclient.responses import GetEmploymentResponse, GetOrganizationResponse, \
    Employment
from sdclient.responses import Person
from sdlon.scripts.log import setup_logging
from sdlon.date_utils import parse_datetime, are_sd_and_mo_dates_equal, format_date
from sdlon.graphql import get_mo_client
from sdlon.scripts.fix_status8 import get_mo_employees


logger = structlog.get_logger(__name__)


def get_sd_employments(
    username: str, password: str, institution_identifier: str
) -> GetEmploymentResponse:
    """
    Get all active employments from SD.

    Args:
        username: the username for the SD API
        password: the password for the SD API
        institution_identifier: the SD institution identifier

    Returns:
        The SD employments
    """

    sd_client = SDClient(username, password)
    sd_employments = sd_client.get_employment(
        GetEmploymentRequest(
            InstitutionIdentifier=institution_identifier,
            EffectiveDate=datetime.now().date(),
            StatusActiveIndicator=True,
            DepartmentIndicator=True,
            EmploymentStatusIndicator=True,
            ProfessionIndicator=True,
            UUIDIndicator=True
        )
    )
    return sd_employments


def get_sd_organization(
        username: str, password: str, institution_identifier: str
) -> GetOrganizationResponse:
    sd_client = SDClient(username, password)
    sd_org = sd_client.get_organization(
        GetOrganizationRequest(
            InstitutionIdentifier=institution_identifier,
            ActivationDate=datetime.now().date(),
            DeactivationDate=datetime.now().date(),
            UUIDIndicator=True
        )
    )
    return sd_org


def get_NY_level_department_map(
        sd_organisation: GetOrganizationResponse,
        too_deep: list[str]
) -> dict[UUID, UUID]:
    """
    Get the NY-levels which the SD employees in the "Afdelings-niveau"s are
    lifted to in the according to the "too deep" rules.
    Args:
        sd_organisation:
        too_deep:

    Returns:
        A dict mapping fra Afdelings-niveau UUID to NY-level UUID
    """
    dep_refs = one(sd_organisation.Organization).DepartmentReference
    afd_dep_refs = [
        dep_ref for dep_ref in dep_refs
        if dep_ref.DepartmentLevelIdentifier == "Afdelings-niveau"
    ]
    afd_to_ny_map = {}
    for dep_ref in afd_dep_refs:
        try:
            parent_dep_ref = one(dep_ref.DepartmentReference)
            while parent_dep_ref.DepartmentLevelIdentifier in too_deep:
                parent_dep_ref = one(parent_dep_ref.DepartmentReference)

            assert parent_dep_ref.DepartmentLevelIdentifier not in too_deep

            afd_to_ny_map[dep_ref.DepartmentUUIDIdentifier] = parent_dep_ref.DepartmentUUIDIdentifier
        except ValueError:
            logger.warn("Found Afdelings-niveau with no parent", dep_ref=dep_ref)

    return afd_to_ny_map


def get_mo_engagements(
    gql_client: GraphQLClient, employee_uuid: UUID | None
) -> dict[str, dict[str, Any]]:
    """
    Get MO engagements for a given employee.

    Args:
        gql_client: the GraphQL client
        employee_uuid: UUID of the employee to get the engagements from

    Returns:
        Dict of the form { eng_user_key: { "eng_data" }}
    """

    if employee_uuid is None:
        return {}

    query = gql("""
        query GetEngagements($uuid: [UUID!]!) {
            engagements(employees: $uuid) {
                objects {
                    current {
                        uuid
                        user_key
                        validity {
                            from
                            to
                        }
                        job_function {
                            name
                        }
                        primary {
                            name
                        }
                    }
                    objects {
                        org_unit {
                            name
                            user_key
                            uuid
                        }
                    }
                }
            }
        }
        """)

    r = gql_client.execute(query, variable_values={"uuid": str(employee_uuid)})

    engagements = {
        engagement["current"]["user_key"]: {
            "uuid": engagement["current"]["uuid"],
            "user_key": engagement["current"]["user_key"],
            "validity": {
                "from": engagement["current"]["validity"]["from"],
                "to": engagement["current"]["validity"]["to"]
            },
            "job_function": engagement["current"]["job_function"]["name"],
            "primary": engagement["current"]["primary"]["name"],
            "org_unit": one(one(engagement["objects"])["org_unit"])
        }
        for engagement in r["engagements"]["objects"]
    }
    return engagements


def get_mo_cpr_to_uuid_map(mo_employees: list[Employee]) -> dict[str, UUID]:
    return {
        employee.cpr_no: employee.uuid
        for employee in mo_employees
    }


def engagement_match(
        sd_emp: Employment,
        sd_ny_dep: UUID,
        mo_eng: dict[str, Any]
) -> (bool, dict):
    match = True
    mismatches: list[str] = []
    mo_ou = mo_eng["org_unit"]
    if not sd_ny_dep == UUID(mo_ou["uuid"]):
        mismatches.append("Unit")
        match = False
    if not sd_emp.Profession.EmploymentName == mo_eng["job_function"]:
        mismatches.append("Job function")
        match = False

    sd_eng_end_date_str = format_date(sd_emp.EmploymentStatus.DeactivationDate)
    if not are_sd_and_mo_dates_equal(sd_eng_end_date_str, mo_eng["validity"]["to"]):
        mismatches.append("End date")
        match = False

    return match, mismatches


@click.command()
@click.option(
    "--username",
    "username",
    envvar="SD_USER",
    required=True,
    help="SD username"
)
@click.option(
    "--password",
    "password",
    envvar="SD_PASSWORD",
    required=True,
    help="SD password"
)
@click.option(
    "--institution-identifier",
    "institution_identifier",
    envvar="SD_INSTITUTION_IDENTIFIER",
    required=True,
    help="SD institution identifier"
)
@click.option(
    "--auth-server",
    "auth_server",
    default="http://keycloak:8080/auth",
    help="Keycloak auth server URL"
)
@click.option(
    "--client-id",
    "client_id",
    default="sdlon",
    help="Keycloak client id"
)
@click.option(
    "--client-secret",
    "client_secret",
    envvar="CLIENT_SECRET",
    required=True,
    help="Keycloak client secret for the DIPEX client"
)
@click.option(
    "--mo-base-url",
    "mo_base_url",
    type=click.STRING,
    default="http://mo:5000",
    help="Base URL for calling MO"
)
@click.option(
    "--use-pickle",
    "use_pickle",
    is_flag=True,
    help="Store SD response locally with pickle and use pickled response "
         "in later runs (useful to avoid unnecessary load on SD during "
         "development)"
)
@click.option(
    "--too-deep",
    "too_deep",
    multiple=True,
    default=("Afdelings-niveau",),
    help="The NY-logic too deep levels",
)
@click.option(
    "--cpr",
    "cpr",
    help="Only make the comparison for this CPR"
)
@click.option(
    "--log-level",
    "log_level",
    default="INFO"
)
def main(
    username: str,
    password: str,
    institution_identifier: str,
    auth_server: str,
    client_id: str,
    client_secret: str,
    mo_base_url: str,
    use_pickle: bool,
    too_deep: tuple[str],
    cpr: str,
    log_level: str,
):
    setup_logging(log_level)
    logger.info("Starting script")

    # To make as few heavy SD as possible during development
    if use_pickle:
        pickle_file_employments = "/tmp/sdlon/sd_employments.bin"
        pickle_file_org = "/tmp/sdlon/sd_org.bin"
        if not pathlib.Path(pickle_file_employments).is_file():
            sd_employments = get_sd_employments(
                username, password, institution_identifier
            )
            sd_org = get_sd_organization(
                username, password, institution_identifier
            )
            with open(pickle_file_employments, "bw") as fp:
                pickle.dump(sd_employments, fp)
            with open(pickle_file_org, "bw") as fp:
                pickle.dump(sd_org, fp)
        with open(pickle_file_employments, "br") as fp:
            sd_employments = pickle.load(fp)
        with open(pickle_file_org, "br") as fp:
            sd_org = pickle.load(fp)
    else:
        sd_employments = get_sd_employments(
            username, password, institution_identifier
        )
        sd_org = get_sd_organization(
            username, password, institution_identifier
        )

    # TODO: verify that SD persons have unique cprs

    gql_client = get_mo_client(
        cast(AnyHttpUrl, auth_server), client_id, client_secret, mo_base_url, 13
    )

    # TODO: move get_mo_employees to separate function
    logger.info("Getting employees from MO...")
    mo_employees = get_mo_employees(gql_client)
    mo_cpr_to_uuid_map = get_mo_cpr_to_uuid_map(mo_employees)
    sd_dep_map = get_NY_level_department_map(sd_org, list(too_deep))

    sd_persons = sd_employments.Person
    if cpr is not None:
        sd_persons = [
            person for person in sd_employments.Person
            if person.PersonCivilRegistrationIdentifier == cpr
        ]

    logger.info("Starting engagement comparisons...")

    diffs = {}
    for sd_person in sd_persons:
        cpr = sd_person.PersonCivilRegistrationIdentifier

        sd_employments = sd_person.Employment
        logger.debug("SD employments for person", sd_employments=sd_employments)

        mo_engagements = get_mo_engagements(
            gql_client,
            mo_cpr_to_uuid_map.get(cpr, None)
        )
        logger.debug("MO engagements for person", engagements=mo_engagements)

        for sd_employment in sd_employments:
            emp_id = sd_employment.EmploymentIdentifier
            logger.info("Checking SD person", cpr=f"{cpr[:6]}-xxxx", emp_id=emp_id)

            key = (cpr, emp_id)
            sd_ny_dep = sd_dep_map[sd_employment.EmploymentDepartment.DepartmentUUIDIdentifier]

            sd_employment_with_ny_ou = deepcopy(sd_employment)
            sd_employment_with_ny_ou.EmploymentDepartment.DepartmentUUIDIdentifier = sd_ny_dep

            mo_engagement = mo_engagements.pop(emp_id, None)
            if mo_engagement is None:
                diffs[key] = {
                    "sd": sd_employment_with_ny_ou,
                    "sd_raw": sd_employment,
                    "mo": None,
                    "mismatches": ["Unit", "Job function", "End date"]
                }
                continue

            match, mismatches = engagement_match(sd_employment, sd_ny_dep, mo_engagement)
            if not match:
                diffs[key] = {
                    "sd": sd_employment_with_ny_ou,
                    "sd_raw": sd_employment,
                    "mo": mo_engagement,
                    "mismatches": mismatches
                }

        # Remaining MO engagements for the user which does not have a
        # corresponding SD employment
        logger.debug("Remaining MO engagements for person", engagements=mo_engagements)
        if mo_engagements:
            for user_key, mo_eng in mo_engagements.items():
                diffs[(cpr, user_key)] = {
                    "sd": None,
                    "sd_raw": None,
                    "mo": mo_eng,
                    "mismatches": ["Unit", "Job function", "End date"]
                }

    logger.info("Writing diffs to file")
    with open("/tmp/sdlon/diffs.bin", "bw") as fp:
        pickle.dump(diffs, fp)

    logger.info("Script finished")


if __name__ == "__main__":
    main()
