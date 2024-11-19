from datetime import datetime
from datetime import timedelta
from uuid import UUID

import click
from gql import gql
from more_itertools import first
from more_itertools import last
from more_itertools import one
from more_itertools import only
from pydantic.main import BaseModel
from tools.log import get_logger
from tools.log import LogLevel
from tools.log import setup_logging

from raclients.graph.client import GraphQLClient
from ra_utils.job_settings import JobSettings

from reports.graphql import get_mo_client

DATE_FORMAT = "%Y-%m-%d"

GET_ADM_UNIT = gql(
    """
    query GetAdmUnit($org_unit: [UUID!]) {
      org_units(filter: { uuids: $org_unit }) {
        objects {
          current {
            name
            uuid
            engagements {
              uuid
            }
            parent {
              uuid
            }
            children {
              uuid
            }
            managers(filter: { org_unit: { uuids: $org_unit } }) {
              person {
                engagements(filter: { org_unit: { uuids: $org_unit } }) {
                  user_key
                }
              }
            }
            addresses(filter: { address_type_user_keys: "Pnummer" }) {
              value
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

GET_MED_UNIT = gql(
    """
    query GetMedUnit($org_unit: [UUID!]) {
      org_units(filter: { uuids: $org_unit }) {
        objects {
          current {
            associations {
              uuid
            }
            children {
              uuid
            }
          }
        }
      }
    }
    """
)


GET_ASSOCIATION = gql(
    """
    query GetAssociation($uuid: [UUID!], $to_date: DateTime) {
      associations(filter: { uuids: $uuid, to_date: $to_date }) {
        objects {
          validities {
            validity {
                from
                to
            }
          }
          current {
            association_type {
              name
              user_key
              uuid
            }
            dynamic_class {
              full_name
              name
              user_key
              uuid
            }
            person {
              cpr_number
            }
          }
        }
      }
    }
    """
)

setup_logging(LogLevel.DEBUG)
logger = get_logger()


class AdmUnitRow(BaseModel):
    person_user_key: str
    cpr: str
    first_name: str
    last_name: str
    email: str
    org_unit: UUID
    is_manager: bool
    eng_start: str
    eng_end: str
    manager_eng_user_key: str
    username: str  # AD username
    job_function: str


class AdmOrgUnitRow(BaseModel):
    name: str
    uuid: UUID
    parent: UUID | None
    pnumber: str


class MedUnitRow(BaseModel):
    cpr: str
    org_unit: UUID
    ass_start: str
    ass_end: str
    role: str
    main_org: str


def process_engagement(
    gql_client: GraphQLClient, eng_uuid: UUID, ou_uuid: UUID, manager_eng_user_key: str
) -> AdmUnitRow:
    logger.debug("Processing engagement", uuid=str(eng_uuid))
    to_date = datetime.now() + timedelta(days=1)

    engagement = gql_client.execute(
        GET_ENGAGEMENT,
        variable_values={
            "uuid": str(eng_uuid),
            "to_date": to_date.strftime(DATE_FORMAT)
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
    email = only(person["addresses"], {}).get("value", "")

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
        manager_eng_user_key=manager_eng_user_key,
        username=first(email.split("@")),
        job_function=current["job_function"]["name"]
    )


def process_adm_unit(
    gql_client: GraphQLClient,
    org_unit: UUID,
    adm_unit_rows: list[AdmUnitRow],
    adm_ou_rows: list[AdmOrgUnitRow]
) -> tuple[list[AdmUnitRow], list[AdmOrgUnitRow]]:
    logger.info("Processing adm unit", uuid=str(org_unit))

    unit = gql_client.execute(
        GET_ADM_UNIT, variable_values={"org_unit": str(org_unit)}
    )
    # Example response:
    #
    # "org_units": {
    #   "objects": [
    #     {
    #       "current": {
    #         "name": "Some name"
    #         "uuid": "869c6187-1a05-4dc9-8881-4803bd9277d6"
    #         "engagements": [
    #           {
    #             "uuid": "83483193-c623-4a59-a0c1-ac8887fba72e"
    #           }
    #         ],
    #         "parent": {
    #            "uuid": "06f200ae-a05e-4fb3-91a4-9f16a0fc0b98"
    #         }
    #         "children": [
    #           {
    #             "uuid": "dbce9dee-3cad-4713-a5ff-466051108c99"
    #           }
    #         ],
    #         "managers": [
    #             {
    #                 "person": [
    #                     {
    #                         "engagements": [
    #                             {
    #                                 "user_key": "54321"
    #                             }
    #                         ]
    #                     }
    #                 ]
    #             }
    #         ]
    #         "addresses": [
    #           {
    #             "value": "1234567890"
    #           }
    #         ]
    #       }
    #     }
    #   ]
    # }

    current = one(unit["org_units"]["objects"])["current"]

    # Engagement data
    engs = [UUID(eng["uuid"]) for eng in current["engagements"]]
    children = [UUID(child["uuid"]) for child in current["children"]]

    manager = only(current["managers"], {})
    manager_person = manager.get("person", [{}])
    manager_eng = only(manager_person).get("engagements", [{}])
    manager_eng_user_key = only(manager_eng, {}).get("user_key", "")

    # Org unit data
    parent_uuid = current.get("parent", {}).get("uuid")
    pnumber = only(current["addresses"], {}).get("value", "")
    adm_ou_row = AdmOrgUnitRow(
        name=current.get("name", ""),
        uuid=UUID(current["uuid"]),
        parent=UUID(parent_uuid) if parent_uuid is not None else None,
        pnumber=pnumber
    )

    adm_ou_rows.append(adm_ou_row)

    for eng in engs:
        adm_unit_row = process_engagement(
            gql_client, eng, org_unit, manager_eng_user_key
        )
        adm_unit_rows.append(adm_unit_row)

    for child in children:
        process_adm_unit(gql_client, child, adm_unit_rows)

    return adm_unit_rows, adm_ou_rows


def process_association(
    gql_client: GraphQLClient, ass_uuid: UUID, ou_uuid: UUID
) -> MedUnitRow:
    logger.debug("Processing association", uuid=str(ass_uuid))
    to_date = datetime.now() + timedelta(days=1)

    engagement = gql_client.execute(
        GET_ASSOCIATION,
        variable_values={
            "uuid": str(ass_uuid),
            "to_date": to_date.strftime(DATE_FORMAT)
        }
    )
    # Example response
    #
    # "associations": {
    #   "objects": [
    #     {
    #       "validities": [
    #         {
    #           "validity": {
    #             "from": "2022-01-06T00:00:00+01:00",
    #             "to": null
    #           }
    #         }
    #       ],
    #       "current": {
    #         "association_type": {
    #           "name": "AMR",
    #           "user_key": "assoc_AMR",
    #           "uuid": "8146e191-0549-45d4-ba3b-cf9a63d80599"
    #         },
    #         "dynamic_class": {
    #           "full_name": "Ej relevant",
    #           "name": "Ej relevant",
    #           "user_key": "na3",
    #           "uuid": "980c9936-af89-4eac-a772-23d1d09eafc0"
    #         },
    #         "person": [
    #           {
    #             "cpr_number": "0101011234"
    #           }
    #         ]
    #       }
    #     }
    #   ]
    # }

    obj = one(engagement["associations"]["objects"])

    ass_start = first(obj["validities"])["validity"]["from"][:10]
    to = last(obj["validities"])["validity"]["to"]
    ass_end = to[:10] if to is not None else ""

    current = obj["current"]
    person = only(current["person"], {})
    cpr = person.get("cpr_number", "")

    return MedUnitRow(
        cpr=cpr,
        org_unit=ou_uuid,
        ass_start=ass_start,
        ass_end=ass_end,
        role=current["association_type"]["name"],
        main_org=current["dynamic_class"]["name"]
    )


def process_med_unit(
    gql_client: GraphQLClient, org_unit: UUID, med_unit_rows: list[MedUnitRow]
) -> list[MedUnitRow]:
    logger.info("Processing med unit", uuid=str(org_unit))

    unit = gql_client.execute(
        GET_MED_UNIT, variable_values={"org_unit": str(org_unit)}
    )
    # Example response:
    #
    # "org_units": {
    #   "objects": [
    #     {
    #       "current": {
    #         "associations": [
    #           {
    #             "uuid": "6c113a45-661f-4ff0-ac92-864f09d707eb"
    #           }
    #         ],
    #         "children": [
    #           {
    #             "uuid": "d32a3f1b-0f63-4afe-980e-e04405545925"
    #           }
    #         ]
    #       }
    #     }
    #   ]
    # }
    current = one(unit["org_units"]["objects"])["current"]

    assocs = [UUID(ass["uuid"]) for ass in current["associations"]]
    children = [UUID(child["uuid"]) for child in current["children"]]

    for ass in assocs:
        med_unit_row = process_association(gql_client, ass, org_unit)
        med_unit_rows.append(med_unit_row)

    for child in children:
        process_med_unit(gql_client, child, med_unit_rows)

    return med_unit_rows


def adm_unit_rows_to_csv(rows: list[AdmUnitRow]) -> list[str]:
    return [
        "Medarbejdernummer,"
        "CPR,"
        "Fornavn,"
        "Efternavn,"
        "Mail,"
        "Afdelingskode,"
        "ErLeder,"
        "Startdato,"
        "Slutdato,"
        "LedersMedarbejdernummer,"
        "Brugernavn,"
        "Titel,"
        "Faggruppe\n"
    ] + [
        (
            f"{r.person_user_key},{r.cpr},{r.first_name},{r.last_name},{r.email},"
            f"{str(r.org_unit)},{str(r.is_manager)},{r.eng_start},"
            f"{r.eng_end},{r.manager_eng_user_key},"
            f"{r.username},{r.job_function},{r.job_function}\n"
        )
        for r in rows
    ]


def med_unit_rows_to_csv(rows: list[MedUnitRow]) -> list[str]:
    return [
        "CPR,"
        "Afdelingskode,"
        "Startdato,"
        "Slutdato,"
        "Hverv,"
        "Hovedorganisation\n"
    ] + [
        f"{r.cpr},{str(r.org_unit)},{r.ass_start},{r.ass_end},{r.role},{r.main_org}\n"
        for r in rows
    ]


def write_csv(path: str, lines: list[str]) -> None:
    with open(path, "w") as fp:
        fp.writelines(lines)


def get_settings(*args, **kwargs) -> JobSettings:
    return JobSettings(*args, **kwargs)


@click.command()
@click.option(
    "--adm-unit-uuid",
    type=click.UUID,
    required=True,
    help="UUID of top level adm unit to process"
)
@click.option(
    "--med-unit-uuid",
    type=click.UUID,
    required=True,
    help="UUID of top level med unit to process"
)
def main(adm_unit_uuid: UUID, med_unit_uuid: UUID) -> None:
    logger.info("Started Safetynet report generation")

    settings = get_settings()

    gql_client = get_mo_client(
        auth_server=settings.crontab_AUTH_SERVER,
        client_id=settings.client_id,
        # Careful - this is not a SecretStr
        client_secret=settings.crontab_CLIENT_SECRET,
        mo_base_url=settings.mora_base,
        gql_version=22,
    )

    # Adm employee report
    logger.info("Generating adm employee report")
    adm_unit_rows = process_adm_unit(gql_client, adm_unit_uuid, [])
    csv_lines = adm_unit_rows_to_csv(adm_unit_rows)
    write_csv("/tmp/adm-unit-engagements.csv", csv_lines)

    # Med employee (based on associations) report
    logger.info("Generating med association report")
    med_unit_rows = process_med_unit(gql_client, med_unit_uuid, [])
    csv_lines = med_unit_rows_to_csv(med_unit_rows)
    write_csv("/tmp/med-unit-associations.csv", csv_lines)

    logger.info("Finished Safetynet report generation")


if __name__ == "__main__":
    main()
