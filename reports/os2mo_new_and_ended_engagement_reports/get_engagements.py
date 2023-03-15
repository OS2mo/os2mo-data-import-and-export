from uuid import UUID
from datetime import datetime, date, timedelta
from typing import List
from typing import Optional

import os
import csv
import json
import time
import shutil
from dateutil import utils
from more_itertools import one
from fastapi.encoders import jsonable_encoder

from gql import gql
from gql.client import SyncClientSession

import pandas as pd

from reports.os2mo_new_and_ended_engagement_reports.config import setup_gql_client
from reports.os2mo_new_and_ended_engagement_reports.config import (
    get_engagement_settings,
)


def read_report_as_json(path_to_file):
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


def gql_query_validity_field(
    validity_from: bool = False, validity_to: bool = False
) -> str:
    """GQL query to return to use as input, depending on what type of engagement is wanted."""
    if validity_from:
        return """query EstablishedEngagements ($engagement_date_to_query_from: DateTime) {
             engagements(from_date: $engagement_date_to_query_from) {
               objects {
                 employee_uuid
                 validity {
                   from
                 }
               }
             }
           }
        """

    if validity_to:
        return """query EstablishedEngagements ($engagement_date_to_query_from: DateTime) {
             engagements(from_date: $engagement_date_to_query_from) {
               objects {
                 employee_uuid
                 validity {
                   to
                 }
               }
             }
           }
        """


def gql_query_persons_details_to_display(
    started_engagement: bool = False, ended_engagement: bool = False
) -> str:
    """GQL query to return to use as input, depending on what type of engagement is wanted."""
    if started_engagement:
        return """
        query PersonEngagementDetails($uuidlist: [UUID!], $email_uuid_list: [UUID!]) {
            employees(uuids: $uuidlist, to_date: null) {
              objects {
                cpr_no
                name
                uuid
                addresses(address_types: $email_uuid_list) {
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
       """

    if ended_engagement:
        return """
            query PersonEngagementDetails($uuidlist: [UUID!], $email_uuid_list: [UUID!]) {
                employees(uuids: $uuidlist) {
                  objects {
                    cpr_no
                    name
                    uuid
                    addresses(address_types: $email_uuid_list) {
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
        """


def established_person_engagements(
    gql_session: SyncClientSession,
    validity_field_from: bool = None,
    validity_field_to: bool = None,
) -> dict:
    """
    Reading all of active engagements with the persons uuid(s) engagement start date through a
    GraphQL call.

    args:
    A GraphQL session to execute the graphQL query.

    Optional param of either "validity_field_from" or "validity_field_to" to
    specify what engagement validity to retrieve data of.

    returns:
    An object of active engagements with persons uuid(s) and dates on persons validity from.
    """
    if validity_field_from:
        graphql_query = gql(gql_query_validity_field(validity_from=True))

    elif validity_field_to:
        graphql_query = gql(gql_query_validity_field(validity_to=True))

    variables = {"engagement_date_to_query_from": date.today().isoformat()}
    response = gql_session.execute(
        graphql_query, variable_values=jsonable_encoder(variables)
    )

    return response


def get_filtered_engagements_for_started_today(gql_query_response: dict) -> List[UUID]:
    """
    Applying filter to get uuids of persons which have started engagements as of today.

    args:
    A graphQL query with payload of active engagements.

    returns:
    A list of eligible uuid(s) of the person(s) that have started their engagements as of today.
    """

    def filter_today(obj):
        return (
            datetime.fromisoformat(one(obj["objects"])["validity"]["from"]).replace(
                tzinfo=None
            )
            == utils.today()
        )

    filtered_dates = filter(
        lambda startdate: one(startdate["objects"])["validity"]["from"],
        gql_query_response["engagements"],
    )

    filtered_engagements_by_today_date = filter(filter_today, filtered_dates)

    extracted_uuids = [
        one(person_uuids["objects"])["employee_uuid"]
        for person_uuids in filtered_engagements_by_today_date
    ]

    return extracted_uuids


def get_filtered_engagements_for_ended_today(gql_query_response: dict) -> List[UUID]:
    """
    Applying filter to get uuids of persons which have ended their engagements as of today.

    args:
    A graphQL query with payload of ended engagements.

    returns:
    A list of eligible uuid(s) of the person(s) that have ended their engagements as of today.
    """

    def filter_to_today(obj):
        return (
            datetime.fromisoformat(one(obj["objects"])["validity"]["to"]).replace(
                tzinfo=None
            )
            == utils.today()
        )

    filtered_dates = filter(
        lambda enddate: one(enddate["objects"])["validity"]["to"],
        gql_query_response["engagements"],
    )

    filtered_engagements_by_today_date = filter(filter_to_today, filtered_dates)

    extracted_uuids = [
        one(person_uuids["objects"])["employee_uuid"]
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
                 address_type {
                   scope
                   uuid
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
        lambda address_type: one(address_type["objects"])["address_type"]["scope"]
        == "EMAIL",
        gql_query_dict["addresses"],
    )
    extracted_email_uuids = [
        one(obj["objects"])["address_type"]["uuid"]
        for obj in filtered_email_address_uuids
    ]

    return extracted_email_uuids


def persons_details_from_engagement(
    gql_session: SyncClientSession,
    uuidlist: List[UUID] | set,
    address_type_uuid_list: List[UUID],
    started_engagement_details: bool = False,
    ended_engagement_details: bool = False,
) -> dict:
    """
    Retrieving all desired details on the person from the filtered engagements through a
    GraphQL call.

    args:
    A GraphQL session to execute the graphQL query.
    List of person uuids to query on.
    List of address type uuids to query on.

    Optional param of either "started_engagement_details" or "ended_engagement_details" to
    specify what type of engagement to retrieve details of.

    returns:
    A dict with all desired details on person by active engagement.
    """
    if started_engagement_details:
        graphql_query = gql(
            gql_query_persons_details_to_display(started_engagement=True)
        )

    elif ended_engagement_details:
        graphql_query = gql(gql_query_persons_details_to_display(ended_engagement=True))

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
    graphql_query = gql(
        """
        query MyQuery {
          employees(from_date: null, to_date: null) {
            uuid
          }
        }
        """
    )

    all_persons = gql_session.execute(graphql_query)

    return all_persons["employees"]


def write_report_as_json(gql_object: List[dict], path_to_file):
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
    dict_data: dict, started: bool = False, ended: bool = False
):
    """
    Mapping fields of payload from engagement to CSV format.

    args:
    A dictionary consisting of a payload with the fields of Person(s) name(s), Person(s) uuid(s),
    User key(s), Organisation unit name(s), CPR number(s), Email(s), Engagement date(s).

    Optional param of either "started" or "ended" to specify what type of engagement to write
    details of.

    returns:
    A CSV format of fields properly mapped to their representative fields.
    """

    def get_ad_it_system_user_key(data_dict: dict) -> Optional[str]:
        """A helper function to extract the user_key of AD It Systems."""
        for data in data_dict["itusers"]:
            if data is not None and data["itsystem"]["name"] == "Active Directory":
                return data["user_key"]
        return None

    def get_org_unit_ancestor(gql_data: dict) -> Optional[str]:
        """In case of multiple root Organisations, this function can
        be used to extract the name of the Organisation."""
        for data in gql_data["engagements"]:
            assert 1 == 1
            if len(data["org_unit"][0]["ancestors"]) == 0:
                assert 2 == 2
                return data["org_unit"][0]["name"]
            if len(data["org_unit"][0]["ancestors"]) >= 1:
                assert 3 == 3
                return data["org_unit"][0]["ancestors"][-1]["name"]
            return None

    out = []
    if started:
        for employee in dict_data["employees"]:
            for obj in employee["objects"]:
                out.append(
                    {
                        "Personens navn": obj["name"],
                        "Personens UUID": obj["uuid"],
                        "Ansættelsessted": obj['engagements'][0]['org_unit'][0]['name']
                        if obj["engagements"]
                        else "Der findes intet fremtidigt engagement for personen",
                        "Ansættelsesdato": datetime.fromisoformat(
                            obj["engagements"][0]["validity"]["from"]).date().isoformat()
                        if obj["engagements"]
                        else "Der findes intet fremtidigt engagement for personen",
                        "Oprettelsesdato": date.today().isoformat(),
                        "CPR": "'{crp}'".format(crp=obj["cpr_no"]) if obj["cpr_no"] else None,
                        "Email": obj["addresses"][0]["name"]
                        if obj["addresses"]
                        else None,
                        "Shortname": get_ad_it_system_user_key(obj),
                        "Organisation": get_org_unit_ancestor(obj),
                    }
                )

    elif ended:
        for employee in dict_data["employees"]:
            for obj in employee["objects"]:
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
                        "Shortname": get_ad_it_system_user_key(obj),
                    }
                )

    return pd.DataFrame(out).to_csv(
        index=False,
        header=True,
        sep=";",
        quoting=csv.QUOTE_ALL,
    )


yesterdays_report_testing = read_report_as_json(
    "reports/os2mo_new_and_ended_engagement_reports/employee_uuids_yesterday.json"
)

todays_report_testing = read_report_as_json(
    "reports/os2mo_new_and_ended_engagement_reports/employee_uuids_today.json"
)


def write_file(contents_of_file, path_to_file):
    """
    A generic way of writing a file.

    args:
    Data with the contents wished to be written. A path to where the file is wanted to be stored.

    returns:
    A written file with the desired contents.
    """
    with open(path_to_file, "w+", encoding="utf-8") as file:
        file.write(contents_of_file)


def display_engagements(
    gql_session: SyncClientSession,
    show_started_engagements: bool = False,
    show_ended_engagements: bool = False,
    testing_show: bool = False,
) -> str:
    """
    Calls upon GraphQL queries and various filters defined in this module, to return all
    relevant data with all the details wanted on an engagement and on a person.

    args:
    A GraphQL session to execute the graphQL queries.
    Optional param of either "show_started_engagements" or "show_ended_engagements" to
    specify what type of engagement details to be returned.

    returns:
    All relevant details on engagements formatted in CSV form.
    """

    # Pulling address types so email uuids can be found.
    address_type_uuids_and_scopes = retrieve_address_types_uuids(gql_session)

    # Finding email uuids.
    list_of_email_uuids = get_email_address_type_uuid_from_gql(
        address_type_uuids_and_scopes
    )

    # Getting active payload with validity field "from" engagements.
    payload_of_started_engagements_objects = established_person_engagements(
        gql_session, validity_field_from=True
    )

    # Getting engagements that have an end-date with validity field "to" engagements.
    payload_of_ended_engagements_objects = established_person_engagements(
        gql_session, validity_field_to=True
    )

    # Finding uuids of persons that have started new engagements.
    list_of_person_uuids_started_engagements = (
        get_filtered_engagements_for_started_today(
            payload_of_started_engagements_objects
        )
    )

    # Finding uuids of persons that have ended engagements.
    list_of_person_uuids_ended_engagements = get_filtered_engagements_for_ended_today(
        payload_of_ended_engagements_objects
    )

    newly_established_uuids_in_mo = get_differences_in_uuids(
        yesterdays_report_testing, todays_report_testing
    )
    assert 2 == 2
    details_new_persons_established_in_mo = persons_details_from_engagement(
        gql_session,
        newly_established_uuids_in_mo,
        list_of_email_uuids,
        started_engagement_details=True,
    )
    assert 1 == 1
    if testing_show:
        testing_stuff_in_csv = convert_person_and_engagement_data_to_csv(
            details_new_persons_established_in_mo, started=True
        )
        assert 99 == 99
        return testing_stuff_in_csv

    # Retrieving details on person with new started engagement.
    details_of_started_engagements = persons_details_from_engagement(
        gql_session,
        list_of_person_uuids_started_engagements,
        list_of_email_uuids,
        started_engagement_details=True,
    )
    assert 2 == 2

    # Retrieving details on person with ended engagement.
    details_of_ended_engagements = persons_details_from_engagement(
        gql_session,
        list_of_person_uuids_ended_engagements,
        list_of_email_uuids,
        ended_engagement_details=True,
    )
    if show_started_engagements:
        # Converting details on new started engagements to CSV.
        started_engagements_data_in_csv = convert_person_and_engagement_data_to_csv(
            details_of_started_engagements, started=True
        )
        return started_engagements_data_in_csv

    if show_ended_engagements:
        # Converting details on ended engagements to csv.
        ended_engagements_data_in_csv = convert_person_and_engagement_data_to_csv(
            details_of_ended_engagements, ended=True
        )
        return ended_engagements_data_in_csv


def get_differences_in_uuids(old_report: List[dict], new_report: List[dict]) -> set:
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

    yesterdays_report = read_report_as_json(
        "reports/os2mo_new_and_ended_engagement_reports/employee_uuids_yesterday.json"
    )

    list_of_all_new_persons = gql_get_all_persons_uuids(gql_session)

    write_report_as_json(
        list_of_all_new_persons,
        "reports/os2mo_new_and_ended_engagement_reports/employee_uuids_today.json",
    )

    todays_report = read_report_as_json(
        "reports/os2mo_new_and_ended_engagement_reports/employee_uuids_today.json"
    )

    new_uuids_appeared_in_mo = get_differences_in_uuids(
        yesterdays_report, todays_report
    )

    assert 100 == 100

    new_engagements_to_write = display_engagements(
        gql_session, show_started_engagements=True
    )
    # Generating a file on newly established engagements.
    write_file(
        new_engagements_to_write,
        "reports/os2mo_new_and_ended_engagement_reports/new_engagements.csv",
        # settings.report_engagements_new_file_path,
    )

    ended_engagements_to_write = display_engagements(
        gql_session, show_ended_engagements=True
    )

    lol_test = display_engagements(gql_session, testing_show=True)

    write_file(lol_test, "reports/os2mo_new_and_ended_engagement_reports/halpme.csv")
    # Generating a file  on ended engagements.

    write_file(
        ended_engagements_to_write,
        "reports/os2mo_new_and_ended_engagement_reports/old_engagements.csv",
        # settings.report_engagements_ended_file_path,
    )



if __name__ == "__main__":
    main()
