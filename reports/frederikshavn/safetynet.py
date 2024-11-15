from datetime import datetime
from uuid import UUID

from gql import gql
from more_itertools import first
from more_itertools import last
from more_itertools import one
from pydantic.main import BaseModel

from raclients.graph.client import GraphQLClient


GET_ADM_UNIT = gql(
    """
    query GetAdmUnit($org_unit: [UUID!]) {
      org_units(filter: { uuids: $org_unit }) {
        objects {
          current {
            engagements {
              uuid
            }
            children {
              uuid
            }
            managers {
              user_key
            }
          }
        }
      }
    }
    """
)

GET_ENGAGEMENT = gql(
    """
    query GetEngagement($uuid: [UUID!], $to_date: DateTime) {
      engagements(filter: { uuids: $uuid, from_date: null, to_date: $to_date }) {
        objects {
          validities {
            validity {
              from
              to
            }
          }
          current {
            user_key
            person {
              cpr_number
              given_name
              surname
              addresses(filter: { address_type_user_keys: "AD-Email" }) {
                value
              }
              manager_roles {
                uuid
              }
            }
            job_function {
              name
            }
          }
        }
      }
    }
    """
)


class AdmUnitRow(BaseModel):
    person_user_key: str
    cpr: str
    first_name: str
    last_name: str
    email: str
    org_unit: UUID
    is_manager: bool
    eng_start: datetime
    eng_end: datetime
    manager_person_user_key: str  # User key for person who is manager of the OU
    username: str  # AD username
    job_function: str


def process_engagement(
    gql_client: GraphQLClient, eng_uuid: UUID, ou_uuid: UUID, manager_user_key: str
) -> AdmUnitRow:
    engagement = gql_client.execute(
        GET_ENGAGEMENT,
        variable_values={
            "uuid": str(eng_uuid),
            "to_date": datetime.now().strftime("%Y-%m-%d")
        }
    )
    # Example response
    #
    # "engagements": {
    #     "objects": [
    #         {
    #             "validities": [
    #                 {
    #                     "validity": {
    #                         "from": "2021-10-22T00:00:00+02:00",
    #                         "to": "2023-10-31T00:00:00+01:00"
    #                     }
    #                 },
    #                 {
    #                     "validity": {
    #                         "from": "2023-11-01T00:00:00+01:00",
    #                         "to": "2025-09-30T00:00:00+02:00"
    #                     }
    #                 }
    #             ],
    #             "current": {
    #                 "person": [
    #                     {
    #                         "cpr_number": "0101011255",
    #                         "given_name": "Bruce",
    #                         "surname": "Lee",
    #                         "addresses": [
    #                             {
    #                                 "value": "bruce@kung.fu"
    #                             }
    #                         ],
    #                         "manager_roles": []
    #                     }
    #                 ],
    #                 "job_function": {
    #                     "name": "Kung Fu Master"
    #                 }
    #             }
    #         }
    #     ]
    # }

    obj = one(engagement["engagements"]["objects"])

    eng_start = first(obj["validities"])["validity"]["from"][:10]
    to = last(obj["validities"])["validity"]["to"]
    eng_end = to[:10] if to is not None else ""

    current = obj["current"]
    person = one(current["person"])
    email = one(person["addresses"])["value"]

    return AdmUnitRow(
        person_user_key=current["user_key"],
        cpr=person["cpr_number"],
        first_name=person["given_name"],
        last_name=person["surname"],
        email=email,
        org_unit=ou_uuid,
        is_manager=True if person["manager_roles"] else False,
        eng_start=eng_start,
        eng_end=eng_end,
        manager_person_user_key=manager_user_key,
        username=first(email.split("@")),
        job_function=current["job_function"]["name"]
    )


def process_adm_unit(
    gql_client: GraphQLClient, org_unit: UUID, adm_unit_rows: list[AdmUnitRow]
) -> list[AdmUnitRow]:
    unit = gql_client.execute(
        GET_ADM_UNIT, variable_values={"org_unit": str(org_unit)}
    )
    # Example response:
    #
    # "org_units": {
    #   "objects": [
    #     {
    #       "current": {
    #         "engagements": [
    #           {
    #             "uuid": "83483193-c623-4a59-a0c1-ac8887fba72e"
    #           }
    #         ],
    #         "children": [
    #           {
    #             "uuid": "dbce9dee-3cad-4713-a5ff-466051108c99"
    #           }
    #         ],
    #         "managers": [
    #           {
    #             "user_key": "123456"
    #           }
    #         ]
    #       }
    #     }
    #   ]
    # }

    current = one(unit["org_units"]["objects"])["current"]

    engs = [UUID(eng["uuid"]) for eng in current["engagements"]]
    children = [UUID(child["uuid"]) for child in current["children"]]
    managers = [manager["user_key"] for manager in current["managers"]]

    for eng in engs:
        adm_unit_row = process_engagement(gql_client, eng, org_unit, one(managers))
        adm_unit_rows.append(adm_unit_row)

    for child in children:
        process_adm_unit(gql_client, child, adm_unit_rows)

    return adm_unit_rows
