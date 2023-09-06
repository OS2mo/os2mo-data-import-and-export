import pathlib
import pickle
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

from sdclient.client import SDClient
from sdclient.requests import GetEmploymentRequest, GetOrganizationRequest
from sdclient.responses import GetEmploymentResponse, GetOrganizationResponse, \
    Employment
from sdclient.responses import Person

from sdlon.graphql import get_mo_client
from sdlon.scripts.fix_status8 import get_mo_employees


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
        parent_dep_ref = one(dep_ref.DepartmentReference)
        while parent_dep_ref.DepartmentLevelIdentifier in too_deep:
            parent_dep_ref = one(parent_dep_ref.DepartmentReference)

        assert parent_dep_ref.DepartmentLevelIdentifier not in too_deep

        afd_to_ny_map[dep_ref.DepartmentUUIDIdentifier] = parent_dep_ref.DepartmentUUIDIdentifier

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
    return match, mismatches


@click.command()
@click.option(
    "--username",
    "username",
    type=click.STRING,
    envvar="SD_USER",
    required=True,
    help="SD username"
)
@click.option(
    "--password",
    "password",
    type=click.STRING,
    envvar="SD_PASSWORD",
    required=True,
    help="SD password"
)
@click.option(
    "--institution-identifier",
    "institution_identifier",
    type=click.STRING,
    envvar="SD_INSTITUTION_IDENTIFIER",
    required=True,
    help="SD institution identifier"
)
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
#    required=True,
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
    default=("Afdelings-niveau",)
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    help="Do not perform any changes in MO"
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
    dry_run: bool,
):
    # To make as few heavy SD as possible during development
    if use_pickle:
        pickle_file_employments = "/tmp/sd_employments.bin"
        pickle_file_org = "/tmp/sd_org.bin"
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

    mo_employees = get_mo_employees(gql_client)
    mo_cpr_to_uuid_map = get_mo_cpr_to_uuid_map(mo_employees)
    sd_dep_map = get_NY_level_department_map(sd_org, list(too_deep))

    diffs = {}
    for sd_person in sd_employments.Person:
        cpr = sd_person.PersonCivilRegistrationIdentifier
        sd_employments = sd_person.Employment
        mo_engagements = get_mo_engagements(
            gql_client,
            mo_cpr_to_uuid_map.get(cpr, None)
        )
        for sd_employment in sd_employments:
            emp_id = sd_employment.EmploymentIdentifier
            mo_engagement = mo_engagements.pop(emp_id, None)
            if mo_engagement is None:
                diffs[cpr] = {
                    "sd": sd_employment,
                    "mo": None,
                    "mismatches": ["MO eng not found"]
                }
                continue
            sd_ny_dep = sd_dep_map[sd_employment.EmploymentDepartment.DepartmentUUIDIdentifier]

            match, mismatches = engagement_match(sd_employment, sd_ny_dep, mo_engagement)
            if not match:
                diffs[cpr] = {
                    "sd": sd_employment,
                    "mo": mo_engagement,
                    "mismatches": mismatches
                }
            if mo_engagements:
                diffs[cpr] = {
                    "sd": None,
                    "mo": mo_engagements,
                    "mismatches": mismatches
                }
    for key in diffs.keys():
        mismatches = diffs[key]["mismatches"]
        print(mismatches)
        if not mismatches == ['MO eng not found']:
            print(diffs[key])


if __name__ == "__main__":
    main()
