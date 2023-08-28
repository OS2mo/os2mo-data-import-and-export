# At the time of writing, the SD importer creates engagements in MO
# even though the corresponding EmploymentStatusCode is 8 ("ophørt")
# in the XML payload from SD. This script fixes this, i.e. the script
# will:
#
# 1) Get all status 8 employments from SD
# 2) Iterate over these and terminate the corresponding active engagements in MO

import pathlib
import pickle
from datetime import datetime
from typing import List
from uuid import UUID

import click
from gql import gql
from more_itertools import one, exactly_n
from raclients.graph.client import GraphQLClient

from sdlon.graphql import get_mo_client
from sdclient.client import SDClient
from sdclient.requests import GetEmploymentRequest
from sdclient.responses import GetEmploymentResponse
from sdclient.responses import Person
from ramodels.mo.employee import Employee

def get_sd_employments(
    username: str, password: str, institution_identifier: str
) -> GetEmploymentResponse:
    """
    Get all passive employments from SD (the query params are very
    specific for what is needed here).

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
            StatusActiveIndicator=False,
            StatusPassiveIndicator=True,
            EmploymentStatusIndicator=True,
        )
    )
    return sd_employments


def get_mo_employees(gql_client: GraphQLClient) -> List[Employee]:
    """
    Get all MO employees

    Args:
        gql_client: the GraphQL client

    Returns:
        List of CPR numbers
    """

    query = gql("""
    query GetEmployees {
        employees {
            objects {
                cpr_no
            }
            uuid
        }
    }
    """)
    r = gql_client.execute(query)

    employees = []
    for employee in r["employees"]:
        try:
            employees.append(
                Employee(
                    cpr_no=one(employee["objects"])["cpr_no"],
                    uuid=employee["uuid"]
                )
            )
        except ValueError:
            print("Found invalid CPR!")
            print(employee)

    return employees


def terminate_engagement(
        gql_client: GraphQLClient,
        engagement_uuid: str,
        termination_date: str
) -> None:
    """
    Terminate a MO engagement.

    Args:
        gql_client: the GraphQL client
        engagement_uuid: UUID of the engagement to terminate
        termination_date: the last day of work for the engagement
    """
    graphql_terminate_engagement = gql(
        """
            mutation TerminateEngagement($input: EngagementTerminateInput!) {
                engagement_terminate(input: $input) {
                    uuid
                }
            }
        """
    )

    gql_client.execute(graphql_terminate_engagement, variable_values={
        "input": {
            "uuid": str(engagement_uuid),
            "to": termination_date
        }
    })


def get_mo_engagements(
    gql_client: GraphQLClient, employee_uuid: UUID
) -> list[dict[str, str]]:
    """
    Get MO engagements for a given employee.

    Args:
        gql_client: the GraphQL client
        employee_uuid: UUID of the employee to get the engagements from

    Returns:
        List of dicts where each dict contains user keys, "from date" and
        UUID of the engagements
    """

    query = gql("""
        query GetEngagements($uuid: [UUID!]!) {
            engagements(employees: $uuid) {
                objects {
                    user_key
                    validity {
                        from
                    }
                }
                uuid
            }
        }
    """)

    r = gql_client.execute(query, variable_values={"uuid": str(employee_uuid)})
    engagements = [
        {
            "uuid": engagement["uuid"],
            "user_key": one(engagement["objects"])["user_key"],
            # Convert back and forth between datetime objects and strings?
            # Nah - it is much easier to just use [:10] for this use case
            "from": one(engagement["objects"])["validity"]["from"][:10]
        }
        for engagement in r["engagements"]
    ]
    return engagements


def has_sd_status8(
    sd_employments: GetEmploymentResponse, cpr: str, employment_identifier: str
) -> bool:
    """
    Return True if the MO employee with the given cpr number and
    employment_identifier is in sd_employments

    Args:
        sd_employments: the passive SD employments
        cpr: the CPR number of the employee
        employment_identifier: the SD employment identifier

    Returns:
        True if the combination of CPR and employment identifier has status 8
        in SD and False otherwise
    """
    def has_cpr_and_employment_identifier(person: Person) -> bool:
        cpr_match = cpr == person.PersonCivilRegistrationIdentifier
        employment_identifier_match = exactly_n(person.Employment, 1, lambda emp: emp.EmploymentIdentifier == employment_identifier)
        return cpr_match and employment_identifier_match

    return exactly_n(sd_employments.Person, 1, has_cpr_and_employment_identifier)


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
    "--use-pickle",
    "use_pickle",
    is_flag=True,
    help="Store SD response locally with pickle and use pickled response "
         "in later runs (useful to avoid unnecessary load on SD during "
         "development)"
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
    dry_run: bool,
):
    # Get the SD status employments
    if use_pickle:
        pickle_file = "/tmp/sd_employments.bin"
        if not pathlib.Path(pickle_file).is_file():
            sd_employments = get_sd_employments(
                username, password, institution_identifier
            )
            with open(pickle_file, "bw") as fp:
                pickle.dump(sd_employments, fp)
        with open(pickle_file, "br") as fp:
            sd_employments = pickle.load(fp)
    else:
        sd_employments = get_sd_employments(
            username, password, institution_identifier
        )

    print("Number of SD employments:", len(sd_employments.Person))

    gql_client = get_mo_client(
        auth_server, client_id, client_secret, mo_base_url, 3
    )
    employees = get_mo_employees(gql_client)

    print("Terminate engagements")
    for employee in employees:
        engagements = get_mo_engagements(gql_client, employee.uuid)
        for eng in engagements:
            terminate = has_sd_status8(sd_employments, employee.cpr_no, eng["user_key"])
            if terminate:
                print(employee.cpr_no, eng["user_key"], terminate)
                if not dry_run:
                    terminate_engagement(gql_client, eng["uuid"], eng["from"])


if __name__ == "__main__":
    main()
