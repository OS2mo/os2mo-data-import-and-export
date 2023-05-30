from uuid import UUID

from gql import gql
from more_itertools import one
from raclients.graph.client import GraphQLClient
from sdlon.models import MOBasePerson

QUERY_GET_EMPLOYEE = gql(
    """
        query GetEmployee($cpr: [CPR!]!) {
            employees(cpr_numbers: $cpr) {
                objects {
                    current {
                        name
                        givenname
                        surname
                        uuid
                    }
                }
            }
        }
    """
)


def get_employee(gql_client: GraphQLClient, cpr: str) -> MOBasePerson | None:
    """
    Get employee from MO

    Args:
        gql_client: The GraphQl client for calling MO
        cpr: The CPR number of the employee

    Returns:
        The MO employee or None if the employee does not exist in MO
    """
    r = gql_client.execute(QUERY_GET_EMPLOYEE, variable_values={"cpr": cpr})

    obj = r["employees"]["objects"]
    if not obj:
        return None

    employee = one(obj)["current"]

    return MOBasePerson(
        cpr=cpr,
        givenname=employee["givenname"],
        surname=employee["surname"],
        name=employee["name"],
        uuid=UUID(employee["uuid"]),
    )
