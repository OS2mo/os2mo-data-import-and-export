# This script generates 4 reports (CSV files) and uploads these to
# Safetynet via SFTP. The reports are
# 1) An engagements report for the ADM organisation
# 2) An org unit report for the ADM organisation
# 3) An association report for the MED organisation
# 4) An org unit report for the MED organisation
from contextlib import contextmanager
from io import StringIO
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

from paramiko import AutoAddPolicy
from paramiko import SFTPClient
from paramiko import SSHClient
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
                  job_function {
                    name
                  }
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
    query GetEngagement($uuid: [UUID!]) {
      engagements(filter: { uuids: $uuid, from_date: null, to_date: null }) {
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
            name
            uuid
            associations {
              uuid
            }
            parent {
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
    query GetAssociation($uuid: [UUID!]) {
      associations(filter: { uuids: $uuid, from_date: null, to_date: null }) {
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


class AdmEngRow(BaseModel):
    """
    Data model for the ADM engagement report
    """
    person_user_key: str
    cpr: str
    first_name: str
    last_name: str
    email: str
    org_unit: UUID
    eng_start: str
    eng_end: str
    manager_eng_user_key: str
    username: str  # AD username
    job_function: str


class AdmOuRow(BaseModel):
    """
    Data model for the ADM org unit report
    """
    name: str
    uuid: UUID
    parent: UUID | None
    pnumber: str


class MedAssRow(BaseModel):
    """
    Data model for the MED association report
    """
    cpr: str
    org_unit: UUID
    ass_start: str
    ass_end: str
    role: str
    main_org: str


class MedOuRow(BaseModel):
    """
    Data model for the MED org unit report
    """
    name: str
    uuid: UUID
    parent: UUID | None


def process_engagement(
    gql_client: GraphQLClient, eng_uuid: UUID, ou_uuid: UUID, manager_eng_user_key: str
) -> AdmEngRow:
    """
    Process a single engagement from an ADM org unit

    Args:
        gql_client: the GraphQL client
        eng_uuid: the UUID of the engagement
        ou_uuid: the UUID of the org unit
        manager_eng_user_key: the user key of the engagement of the manager of the OU

    Returns:
        Data for the engagement
    """
    logger.debug("Processing engagement", uuid=str(eng_uuid))

    engagement = gql_client.execute(
        GET_ENGAGEMENT,
        variable_values={
            "uuid": str(eng_uuid),
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
    #                         ]
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
    email = first(person["addresses"], {}).get("value", "")
    cpr = person["cpr_number"] if person["cpr_number"] is not None else ""

    return AdmEngRow(
        person_user_key=current["user_key"],
        cpr=cpr,
        first_name=person["given_name"],
        last_name=person["surname"],
        email=email,
        org_unit=ou_uuid,
        eng_start=eng_start,
        eng_end=eng_end,
        manager_eng_user_key=manager_eng_user_key,
        username=first(email.split("@")),
        job_function=current["job_function"]["name"]
    )


def process_adm_unit(
    gql_client: GraphQLClient,
    org_unit: UUID,
    adm_eng_rows: list[AdmEngRow],
    adm_ou_rows: list[AdmOuRow]
) -> tuple[list[AdmEngRow], list[AdmOuRow]]:
    """
    Recursive function for processing the OU data and engagement data in an
    org unit from the ADM organisation. The function will traverse the entire
    OU-tree from the provided root node.

    Args:
        gql_client: the GraphQL client
        org_unit: the root org unit to process
        adm_eng_rows: list of engagement data to append new data to
        adm_ou_rows: list of OU data to append new data to

    Returns:
        List of engagement data and list of OU data
    """
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
    #                                 "job_function": {
    #                                   "name": "Leder"
    #                                 }
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
    try:
        manager_eng_user_key = only(manager_eng, {}).get("user_key", "")
    except ValueError:
        # The manager has more than one engagement in the same unit
        manager_eng_user_key = one(
            eng["user_key"]
            for eng in manager_eng
            if "leder" in eng["job_function"]["name"].lower()
        )

    # Org unit data
    parent_uuid = current.get("parent", {}).get("uuid")
    pnumber = only(current["addresses"], {}).get("value", "")
    adm_ou_row = AdmOuRow(
        name=current.get("name", ""),
        uuid=UUID(current["uuid"]),
        parent=UUID(parent_uuid) if parent_uuid is not None else None,
        pnumber=pnumber
    )

    adm_ou_rows.append(adm_ou_row)

    for eng in engs:
        adm_eng_row = process_engagement(
            gql_client, eng, org_unit, manager_eng_user_key
        )
        adm_eng_rows.append(adm_eng_row)

    for child in children:
        process_adm_unit(gql_client, child, adm_eng_rows, adm_ou_rows)

    return adm_eng_rows, adm_ou_rows


def process_association(
    gql_client: GraphQLClient, ass_uuid: UUID, ou_uuid: UUID
) -> MedAssRow:
    """
    Process a single association from an ADM org unit

    Args:
        gql_client: the GraphQL client
        ass_uuid: the UUID of the engagement
        ou_uuid: the UUID of the org unit

    Returns:
        Data for the association
    """
    logger.debug("Processing association", uuid=str(ass_uuid))

    engagement = gql_client.execute(
        GET_ASSOCIATION,
        variable_values={
            "uuid": str(ass_uuid),
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

    dynamic_class = current.get("dynamic_class", {})
    main_org = dynamic_class.get("name", "") if dynamic_class is not None else ""

    return MedAssRow(
        cpr=cpr,
        org_unit=ou_uuid,
        ass_start=ass_start,
        ass_end=ass_end,
        role=current["association_type"]["name"],
        main_org=main_org
    )


def process_med_unit(
    gql_client: GraphQLClient,
    org_unit: UUID,
    med_ass_rows: list[MedAssRow],
    med_ou_rows: list[MedOuRow]
) -> tuple[list[MedAssRow], list[MedOuRow]]:
    """
    Recursive function for processing the OU data and engagement data in an
    org unit from the MED organisation. The function will traverse the entire
    OU-tree from the provided root node.

    Args:
        gql_client: the GraphQL client
        org_unit: the root org unit to process
        med_ass_rows: list of association data to append new data to
        med_ou_rows: list of OU data to append new data to

    Returns:
        List of association data and list of OU data
    """
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
    #         "name": "Some name",
    #         "uuid": "a30e42a2-7334-4ffa-bf73-51ed20638511"
    #         "associations": [
    #           {
    #             "uuid": "6c113a45-661f-4ff0-ac92-864f09d707eb"
    #           }
    #         ],
    #         "parent": {
    #            "uuid": "06f200ae-a05e-4fb3-91a4-9f16a0fc0b98"
    #         }
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

    # Association data
    assocs = [UUID(ass["uuid"]) for ass in current["associations"]]
    children = [UUID(child["uuid"]) for child in current["children"]]

    # Org unit data
    parent_uuid = current.get("parent", {}).get("uuid")
    med_ou_row = MedOuRow(
        name=current.get("name", ""),
        uuid=UUID(current["uuid"]),
        parent=UUID(parent_uuid) if parent_uuid is not None else None,
    )

    med_ou_rows.append(med_ou_row)

    for ass in assocs:
        med_ass_row = process_association(gql_client, ass, org_unit)
        med_ass_rows.append(med_ass_row)

    for child in children:
        process_med_unit(gql_client, child, med_ass_rows, med_ou_rows)

    return med_ass_rows, med_ou_rows


def adm_eng_rows_to_csv_lines(rows: list[AdmEngRow]) -> list[str]:
    """
    Convert ADM engagement data models to CSV
    """
    return [
        "Medarbejdernummer||"
        "CPR||"
        "Fornavn||"
        "Efternavn||"
        "Mail||"
        "Afdelingskode||"
        "Startdato||"
        "Slutdato||"
        "LedersMedarbejdernummer||"
        "Brugernavn||"
        "Titel||"
        "Faggruppe\n"
    ] + [
        (
            f"{r.person_user_key}||"
            f"{r.cpr}||"
            f"{r.first_name}||"
            f"{r.last_name}||"
            f"{r.email}||"
            f"{str(r.org_unit)}||"
            f"{r.eng_start}||"
            f"{r.eng_end}||"
            f"{r.manager_eng_user_key}||"
            f"{r.username}||"
            f"{r.job_function}||"
            f"{r.job_function}\n"
        )
        for r in rows
    ]


def med_ass_rows_to_csv_lines(rows: list[MedAssRow]) -> list[str]:
    """
    Convert MED association data models to CSV
    """
    return [
        "CPR||"
        "Afdelingskode||"
        "Startdato||"
        "Slutdato||"
        "Hverv||"
        "Hovedorganisation\n"
    ] + [
        (
            f"{r.cpr}||"
            f"{str(r.org_unit)}||"
            f"{r.ass_start}||"
            f"{r.ass_end}||"
            f"{r.role}||"
            f"{r.main_org}\n"
        )
        for r in rows
    ]


def adm_ou_rows_to_csv_lines(rows: list[AdmOuRow]) -> list[str]:
    """
    Convert ADM org unit data models to CSV
    """
    return [
        "Afdelingsnavn||"
        "Afdelingskode||"
        "Forældreafdelingskode||"
        "Pnummer\n"
    ] + [
        (
            f"{r.name}||"
            f"{str(r.uuid)}||"
            f"{str(r.parent) if r.parent is not None else ''}||"
            f"{r.pnumber}\n"
        )
        for r in rows
    ]


def med_ou_rows_to_csv_lines(rows: list[MedOuRow]) -> list[str]:
    """
    Convert MED org unit data models to CSV
    """
    return [
        "Afdelingsnavn||"
        "Afdelingskode||"
        "Forældreafdelingskode\n"
    ] + [
        (
            f"{r.name}||"
            f"{str(r.uuid)}||"
            f"{str(r.parent) if r.parent is not None else ''}\n"
        )
        for r in rows
    ]


def write_csv(path: str, lines: list[str]) -> None:
    with open(path, "w") as fp:
        fp.writelines(lines)


def get_settings(*args, **kwargs) -> JobSettings:
    return JobSettings(*args, **kwargs)


@contextmanager
def _ssh_client(hostname: str, port: int, username: str, password: str) -> SSHClient:
    ssh_client = SSHClient()
    try:
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        ssh_client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            look_for_keys=False
        )
        yield ssh_client
    finally:
        ssh_client.close()


@contextmanager
def sftp_client(hostname: str, port: int, username: str, password: str) -> SFTPClient:
    with _ssh_client(hostname, port, username, password) as ssh_client:
        sftp_client_: SFTPClient = ssh_client.open_sftp()
        try:
            yield sftp_client_
        finally:
            sftp_client_.close()


def upload_csv(
    hostname: str,
    port: int,
    username: str,
    password: str,
    remote_path: str,
    csv_lines: list[str]
) -> None:
    upload_str = "".join(csv_lines)
    with sftp_client(hostname, port, username, password) as client:
        client.putfo(StringIO(upload_str), remote_path, confirm=False)


@click.command()
@click.option(
    "--adm-unit-uuid",
    type=click.UUID,
    help="UUID of top level adm unit to process"
)
@click.option(
    "--med-unit-uuid",
    type=click.UUID,
    help="UUID of top level med unit to process"
)
@click.option(
    "--skip-upload",
    is_flag=True,
    help="Skip SFTP upload (nice for debugging)"
)
def main(adm_unit_uuid: UUID, med_unit_uuid: UUID, skip_upload: bool) -> None:
    logger.info("Started Safetynet report generation")

    settings = get_settings()

    if not adm_unit_uuid:
        adm_unit_uuid = UUID(settings.reports_safetynet_adm_unit_uuid)
    if not med_unit_uuid:
        med_unit_uuid = UUID(settings.reports_safetynet_med_unit_uuid)

    gql_client = get_mo_client(
        auth_server=settings.crontab_AUTH_SERVER,
        client_id=settings.client_id,
        # Careful - this is not a SecretStr
        client_secret=settings.crontab_CLIENT_SECRET,
        mo_base_url=settings.mora_base,
        gql_version=22,
    )

    sftp_settings = (
        settings.reports_safetynet_sftp_hostname,
        settings.reports_safetynet_sftp_port,
        settings.reports_safetynet_sftp_username,
        settings.reports_safetynet_sftp_password,
    )

    # Adm employee report
    logger.info("Generating adm employee report")
    adm_eng_rows, adm_ou_rows = process_adm_unit(gql_client, adm_unit_uuid, [], [])
    csv_lines = adm_eng_rows_to_csv_lines(adm_eng_rows)
    if skip_upload:
        write_csv("/tmp/adm-engagements.csv", csv_lines)
    else:
        upload_csv(*sftp_settings, "adm-engagements.csv", csv_lines)

    # Med employee (based on associations) report
    logger.info("Generating med association report")
    med_ass_rows, med_ou_rows = process_med_unit(gql_client, med_unit_uuid, [], [])
    csv_lines = med_ass_rows_to_csv_lines(med_ass_rows)
    if skip_upload:
        write_csv("/tmp/med-associations.csv", csv_lines)
    else:
        upload_csv(*sftp_settings, "med-associations.csv", csv_lines)

    # Adm OU report
    logger.info("Generating adm OU report")
    csv_lines = adm_ou_rows_to_csv_lines(adm_ou_rows)
    if skip_upload:
        write_csv("/tmp/adm-org-units.csv", csv_lines)
    else:
        upload_csv(*sftp_settings, "adm-org-units.csv", csv_lines)

    # Med OU report
    logger.info("Generating MED OU report")
    csv_lines = med_ou_rows_to_csv_lines(med_ou_rows)
    if skip_upload:
        write_csv("/tmp/med-org-units.csv", csv_lines)
    else:
        upload_csv(*sftp_settings, "med-org-units.csv", csv_lines)

    logger.info("Finished Safetynet report generation")


if __name__ == "__main__":
    main()
