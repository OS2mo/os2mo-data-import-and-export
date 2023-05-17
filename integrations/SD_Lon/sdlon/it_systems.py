from functools import cache
from uuid import UUID

from gql import gql
from more_itertools import one

from raclients.graph.client import GraphQLClient

QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID = gql(
    """
    query GetItSystems {
        itsystems(user_keys: "SD til AD") {
            objects {
                uuid
            }
        }
    }            
"""
)


@cache
def get_sd_to_ad_it_system_uuid(gql_client: GraphQLClient) -> UUID:
    r = gql_client.execute(QUERY_GET_SD_TO_AD_IT_SYSTEM_UUID)
    return UUID(one(r["itsystems"]["objects"])["uuid"])


def get_employee_itsystems(
    gql_client: GraphQLClient, employee_uuid: UUID
) -> list[UUID]:
    """
    Get the IT-systems for an employee
    Args:
        gql_client: The GraphQL client for calling MO
        employee_uuid: The employee UUID

    Returns:
        List of UUIDs of the employees IT-systems
    """

    query = gql(
        """
        query GetEmployeeItSystems($uuid: [UUID!]!) {
            employees(uuids: $uuid) {
                objects {
                    current {
                        itusers {
                            itsystem {
                                uuid
                            }
                        }
                    }
                }
            }
        }
    """
    )

    r = gql_client.execute(query, variable_values={"uuid": str(employee_uuid)})

    it_users = one(r["employee"]["objects"])["current"]["itusers"]
    it_system_uuids = [UUID(it_user["itsystem"]["uuid"]) for it_user in it_users]

    return it_system_uuids


def add_it_system_to_employee(
    gql_client: GraphQLClient, employee_uuid: UUID, it_system_uuid: UUID
) -> None:
    mutation = gql(
        """
        mutation MyMutation($input: ITUserCreateInput!) {
            ituser_create(input: $input) {
                uuid
            }
        }
    """
    )

    gql_client.execute(
        mutation,
        variable_values={
            "input": {
                "user_key": "SD til AD",
                "itsystem": str(it_system_uuid),
                "validity": {"from": "2023-05-01"},
                "person": str(employee_uuid),
            }
        },
    )
