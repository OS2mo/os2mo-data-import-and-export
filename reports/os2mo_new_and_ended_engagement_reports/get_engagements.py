from uuid import UUID
from datetime import datetime, date
from typing import List

import csv
import json
from dateutil import utils
from more_itertools import one
from fastapi.encoders import jsonable_encoder

from gql import gql
from gql.client import SyncClientSession

import pandas as pd

from raclients.upload import file_uploader

from reports.os2mo_new_and_ended_engagement_reports.config import setup_gql_client
from reports.os2mo_new_and_ended_engagement_reports.config import EngagementSettings
from reports.os2mo_new_and_ended_engagement_reports.config import (
    get_engagement_settings,
)


def read_report_as_json(path_to_file: str) -> List[dict[str, str]]:
    """
    A generic way to read content in JSON format.

    args:
    A path to where the file to read is stored.

    returns:
    A written file with the contents written as JSON.

    example of contents:
    [{"uuid": "0004b952-a513-430b-b696-8d393d7eb2bb"}, , {"uuid": "002a1aed-d015-4b86-86a4-c37cd8df1e18"},
    {"uuid": "00556594-7be8-4c57-ba0a-9d2adefc8d1c"}, {"uuid": "00973369-2d8f-4120-bbaf-75f0e0f38534"}]
    """

    with open(path_to_file, "r") as infile:
        read_json_object = json.load(infile)

    return read_json_object


def get_gql_query_validity_to(
    gql_session: SyncClientSession,
) -> dict:
    """Returns a GQL payload on engagements with validity till today."""
    graphql_query = gql(
    """query EstablishedEngagements ($engagement_date_to_query_from: DateTime) {
            engagements(filter: { from_date: $engagement_date_to_query_from }) {
                objects {
                    validities {
                        employee_uuid
                        validity {
                            to
                        }
                    }
                }
            }
        }
    """
    )

    return gql_session.execute(
        graphql_query, {"engagement_date_to_query_from": date.today().isoformat()}
    )


def gql_query_persons_details_to_display(
    show_new_persons: bool = False, show_ended_engagements: bool = False
) -> str:
    """GQL query to return to use as input, depending on what type of engagement is wanted."""
    if show_new_persons:
        return """
        query PersonEngagementDetails($uuidlist: [UUID!], $email_uuid_list: [UUID!]) {
            employees(filter: { uuids: $uuidlist, to_date: null }) {
                objects {
                    validities {
                        cpr_number
                        name
                        uuid
                        addresses(filter: { address_types: $email_uuid_list }) {
                            name
                        }
                        engagements {
                            org_unit {
                                name
                                ancestors {
                                    user_key
                                    uuid
                                    name
                                }
                            }
                            validity {
                                from
                            }
                        }
                        itusers {
                            user_key
                            itsystem {
                                name
                            }
                        }
                    }
                }
            }
        }
        """

    if show_ended_engagements:
        return """
        query PersonEngagementDetails($uuidlist: [UUID!], $email_uuid_list: [UUID!]) {
            employees(filter: { uuids: $uuidlist }) {
                objects {
                    validities {
                        cpr_number
                        name
                        uuid
                        addresses(filter: { address_types: $email_uuid_list }) {
                            name
                        }
                        engagements {
                            org_unit {
                                name
                                ancestors {
                                    user_key
                                    uuid
                                    name
                                }
                            }
                            validity {
                                to
                            }
                        }
                        itusers {
                            user_key
                            itsystem {
                                name
                            }
                        }
                    }
                }
            }
        }
        """


def get_filtered_engagements_for_ended_today(gql_query_response: dict) -> List[UUID]:
    """
    Applying filter to get uuids of persons which have ended their engagements as of today.

    args:
    A graphQL query with payload of ended engagements.

    returns:
    A list of eligible uuid(s) of the person(s) that have ended their engagements as of today.
    """

    def filter_to_today(obj: dict):
        return (
            datetime.fromisoformat(one(obj["validities"])["validity"]["to"]).replace(
                tzinfo=None
            )
            == utils.today()
        )

    filtered_dates = filter(
        lambda enddate: one(enddate["validities"])["validity"]["to"],
        gql_query_response["engagements"]["objects"],
    )

    filtered_engagements_by_today_date = filter(filter_to_today, filtered_dates)

    extracted_uuids = [
        one(one(person_uuids["objects"])["validities"])["employee_uuid"]
        for person_uuids in filtered_engagements_by_today_date
    ]

    return extracted_uuids


def retrieve_address_types_uuids(gql_session: SyncClientSession) -> dict:
    """
    Reading all types of addresses through a
    GraphQL call.

    args:
    A GraphQL session to execute the graphQL query.

    returns:
    An object containing uuid and scope on all active addresses.
    """
    graphql_query = gql(
        """query GetAddressTypes {
            addresses {
                objects {
                    validities {
                        address_type {
                            scope
                            uuid
                        }
                    }
                }
            }
        }
        """
    )

    response = gql_session.execute(graphql_query)

    return response


def get_email_address_type_uuid_from_gql(gql_query_dict: dict) -> list:
    """
    Filtering through address types to retrieve uuids of addresses with scope of email.

    args:
    A graphQL query with payload on address types.

    returns:
    A list of all uuids with scope of email.
    """
    filtered_email_address_uuids = filter(
        lambda address_type: one(address_type["validities"])["address_type"]["scope"]
        == "EMAIL",
        gql_query_dict["addresses"]["objects"],
    )
    extracted_email_uuids = [
        one(obj["validities"])["address_type"]["uuid"]
        for obj in filtered_email_address_uuids
    ]

    return extracted_email_uuids


def persons_details_from_engagement(
    gql_session: SyncClientSession,
    uuidlist: List[UUID] | set[UUID],
    address_type_uuid_list: List[UUID],
    person_details: bool = False,
    ended_engagement_details: bool = False,
) -> dict:
    """
    Retrieving all desired details on the person from the filtered engagements through a
    GraphQL call.

    args:
    A GraphQL session to execute the graphQL query.
    List of person uuids to query on.
    List of address type uuids to query on.

    Optional param of either "person_details" or "ended_engagement_details" to
    specify what type of engagement to retrieve details of.

    returns:
    A dict with all desired details on person by active engagement.
    """
    if person_details:
        graphql_query = gql(gql_query_persons_details_to_display(show_new_persons=True))

    elif ended_engagement_details:
        graphql_query = gql(
            gql_query_persons_details_to_display(show_ended_engagements=True)
        )

    variables = {"uuidlist": uuidlist, "email_uuid_list": address_type_uuid_list}
    response = gql_session.execute(
        graphql_query, variable_values=jsonable_encoder(variables)
    )

    return response


def gql_get_all_persons_uuids(gql_session: SyncClientSession) -> List[dict]:
    """
    Runs a query to return a list of all person uuids.

    args:
    A GraphQL session to execute the graphQL query.

    returns:
    A list of objects containing key value pairs of uuid(s).

    example:
    [{'uuid': '0004b952-a513-430b-b696-8d393d7eb2bb'},
     {'uuid': '002a1aed-d015-4b86-86a4-c37cd8df1e18'},
     {'uuid': '00556594-7be8-4c57-ba0a-9d2adefc8d1c'}]
    """
    # TODO Make these return as set of uuids rather than a list of dict.
    # {uuid1, uuid2, uuid3}
    graphql_query = gql(
        """
        query MyQuery {
            employees(filter: { from_date: null, to_date: null }) {
                objects {
                    uuid
                }
            }
        }
        """
    )

    all_persons = gql_session.execute(graphql_query)
    # return {x['uuid'] for x in all_persons}
    return all_persons["employees"]["objects"]


def write_report_as_json(gql_object: List[dict], path_to_file: str):
    """
    Function for writing content in JSON format.

    args:
    Data with the contents wished to be written. A path to where the file is wanted to be stored.

    returns:
    A written file with the contents written as JSON.

    example of contents:
    [{"uuid": "0004b952-a513-430b-b696-8d393d7eb2bb"}, , {"uuid": "002a1aed-d015-4b86-86a4-c37cd8df1e18"},
    {"uuid": "00556594-7be8-4c57-ba0a-9d2adefc8d1c"}, {"uuid": "00973369-2d8f-4120-bbaf-75f0e0f38534"}]
    """
    with open(path_to_file, "w") as outfile:
        json.dump(gql_object, outfile, sort_keys=True)


def convert_person_and_engagement_data_to_csv(
    dict_data: dict,
    persons_data_to_csv: bool = False,
    ended_engagements_data_to_csv: bool = False,
):
    """
    Mapping fields of payload from engagement to CSV format.

    args:
    A dictionary consisting of a payload with the fields of Person(s) name(s), Person(s) uuid(s),
    User key(s), Organisation unit name(s), CPR number(s), Email(s), Engagement date(s).

    Optional param of either "persons_data_to_csv" or "ended_engagements_data_to_csv" to specify
    what kind of detail to write.

    returns:
    A CSV format of fields properly mapped to their representative fields.
    """

    def get_ad_it_system_user_key(data_dict: dict) -> str | None:
        """A helper function to extract the user_key of AD It Systems."""
        for data in data_dict:
            if data is not None and data["itsystem"]["name"] == "Active Directory":
                return data["user_key"]
        return None

    def get_org_unit_ancestor(gql_data: dict) -> str | None:
        """In case of multiple root Organisations, this function can
        be used to extract the name of the Organisation."""
        for data in gql_data["engagements"]:
            if len(data["org_unit"][0]["ancestors"]) == 0:
                return data["org_unit"][0]["name"]
            if len(data["org_unit"][0]["ancestors"]) >= 1:
                return data["org_unit"][0]["ancestors"][-1]["name"]
            return None

    out = []
    if persons_data_to_csv:
        for employee in dict_data["employees"]["objects"]:
            for obj in employee["validities"]:
                out.append(
                    {
                        "Personens navn": obj["name"],
                        "Personens UUID": obj["uuid"],
                        "Ansættelsessted": obj["engagements"][0]["org_unit"][0]["name"]
                        if obj["engagements"]
                        else "Der findes intet fremtidigt engagement for personen",
                        "Ansættelsesdato": datetime.fromisoformat(
                            obj["engagements"][0]["validity"]["from"]
                        )
                        .date()
                        .isoformat()
                        if obj["engagements"]
                        else "Der findes intet fremtidigt engagement for personen",
                        "Oprettelsesdato": date.today().isoformat(),
                        "CPR": "'{cpr}'".format(cpr=obj["cpr_number"])
                        if obj["cpr_number"]
                        else None,
                        "Email": obj["addresses"][0]["name"]
                        if obj["addresses"]
                        else None,
                        "Shortname": get_ad_it_system_user_key(obj["itusers"]) if obj["itusers"] is not None else None,
                        "Organisation": get_org_unit_ancestor(obj),
                    }
                )

    elif ended_engagements_data_to_csv:
        for employee in dict_data["employees"]["objects"]:
            for obj in employee["validities"]:
                out.append(
                    {
                        "Personens navn": obj["name"],
                        "Personens UUID": obj["uuid"],
                        "Ansættelsessted": obj["engagements"][0]["org_unit"][0]["name"],
                        "Ansættelsesudløbsdato": obj["engagements"][0]["validity"][
                            "to"
                        ],
                        "Email": obj["addresses"][0]["name"]
                        if obj["addresses"]
                        else None,
                        "Shortname": get_ad_it_system_user_key(obj["itusers"]) if obj["itusers"] is not None else None,
                    }
                )

    return pd.DataFrame(out).to_csv(
        index=False,
        header=True,
        sep=";",
        quoting=csv.QUOTE_ALL,
    )


def write_file(settings: EngagementSettings, contents_of_file: str, path_to_file: str):
    """Upload a file to OS2mo.

    args:
    Data with the contents wished to be written. A path to where the file is wanted to be stored.

    returns:
    A written file with the desired contents.
    """
    with file_uploader(settings, path_to_file) as filename:
        with open(filename, "w", encoding="utf-8") as file:
            file.write(contents_of_file)


def get_differences_in_uuids(
    old_report: List[dict], new_report: List[dict]
) -> set[UUID]:
    """
    Takes two lists of objects and unpacks each into a set. These sets are each
    compared to each-other to find differences. These differences would indicate
    changes in uuid(s) relative to the reports' day; meaning new persons have
    been created, or old persons have been removed.

    args:
    A list of dict(s) with key value pairs.

    returns:
    A set of uuid(s) only.

    example:
    "{'ffbe5804-cf13-450a-a41b-47865e355a15'}"
    """
    old_report_json_set = {uuid["uuid"] for uuid in old_report}

    new_report_json_set = {uuid["uuid"] for uuid in new_report}

    # This might be useful in the future. As of now though, only new entries are needed.
    previous_uuids_already_in_mo = old_report_json_set.difference(new_report_json_set)

    new_uuids_appear_from_today = new_report_json_set.difference(old_report_json_set)

    return new_uuids_appear_from_today


def main() -> None:
    settings = get_engagement_settings()
    settings.start_logging_based_on_settings()
    gql_session = setup_gql_client(settings=settings)
    try:  # Read report from yesterday and store the data in variable.
        yesterdays_report = read_report_as_json(
            "reports/os2mo_new_and_ended_engagement_reports/employee_uuids.json",
            # settings.yesterdays_json_report_path
        )
        print("Read JSON uuids from yesterdays")

    except FileNotFoundError:
        yesterdays_report = []
        print("No files found from yesterday")

    # Get uuids on all persons.
    list_of_all_persons = gql_get_all_persons_uuids(gql_session)

    # Read the report written today with the uuids from all persons.
    todays_report = list_of_all_persons

    # Find uuid difference in reports from yesterday and from today.
    # These must be uuids on all new persons established in MO.
    set_of_newly_established_uuids_in_mo = get_differences_in_uuids(
        yesterdays_report, todays_report
    )

    # Pulling address types so email uuids can be found.
    address_type_uuids_and_scopes = retrieve_address_types_uuids(gql_session)

    # Finding email uuids.
    list_of_email_uuids = get_email_address_type_uuid_from_gql(
        address_type_uuids_and_scopes
    )

    # Getting engagements that have an end-date with validity field "to" engagements.
    payload_of_ended_engagements_objects = get_gql_query_validity_to(gql_session)

    # Finding uuids of persons that have ended engagements.
    list_of_person_uuids_ended_engagements = get_filtered_engagements_for_ended_today(
        payload_of_ended_engagements_objects
    )

    # Finding relevant details on new persons from GraphQL calls.
    details_of_new_persons_established_in_mo = persons_details_from_engagement(
        gql_session,
        set_of_newly_established_uuids_in_mo,
        list_of_email_uuids,
        person_details=True,
    )

    print("Finding all relevant details on new persons today in MO")

    # Finding relevant details on ended engagements from GraphQL calls.
    details_of_ended_engagements = persons_details_from_engagement(
        gql_session,
        list_of_person_uuids_ended_engagements,
        list_of_email_uuids,
        ended_engagement_details=True,
    )

    print("Finding all relevant details on ended engagements today in MO")

    # Details on new persons converted to CSV.
    new_persons_in_mo_csv_data_to_write = convert_person_and_engagement_data_to_csv(
        details_of_new_persons_established_in_mo, persons_data_to_csv=True
    )

    # Details on ended engagements converted to CSV.
    ended_engagements_in_mo_csv_data_to_write = (
        convert_person_and_engagement_data_to_csv(
            details_of_ended_engagements, ended_engagements_data_to_csv=True
        )
    )

    # Write CSV report on all new persons.
    write_file(
        settings,
        new_persons_in_mo_csv_data_to_write,
        settings.report_new_persons_file_path,
    )

    print("Wrote CSV report for new persons in MO today")

    # Write CSV report on all ended engagements.
    write_file(
        settings,
        ended_engagements_in_mo_csv_data_to_write,
        settings.report_ended_engagements_file_path,
    )

    print("Wrote CSV report for ended engagements in MO today")
    print("Report successfully made!")

    # Write a report for today with the uuids from all persons.
    write_report_as_json(
        list_of_all_persons,
        "reports/os2mo_new_and_ended_engagement_reports/employee_uuids.json",
        # settings.todays_json_report_path
    )

    print("Wrote JSON uuids for today")


if __name__ == "__main__":
    main()
