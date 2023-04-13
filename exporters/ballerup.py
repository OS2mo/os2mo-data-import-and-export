#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
"""
Helper class to make a number of pre-defined queries into MO
"""
import os
import time
from typing import Any

import common_queries as cq
from anytree import PreOrderIter
from gql.client import SyncClientSession
from more_itertools import first
from more_itertools import one
from os2mo_helpers.mora_helpers import MoraHelper

from exporters.common_query_exports.common_graphql import get_managers_for_export
from exporters.common_query_exports.config import get_common_query_export_settings
from exporters.common_query_exports.config import setup_gql_client

MORA_BASE = os.environ.get("MORA_BASE", "http://localhost:5000")


def export_udvalg(mh, nodes, filename, fieldnames, org_types):
    """Traverses a tree of OUs, find members of 'udvalg'
    :param mh: Instance of MoraHelper to do the actual work
    :param nodes: The nodes of the OU tree
    :param fieldnames: Fieldnames for structur of the 'udvalg'
    :param org_types: Org types belong to this kind of 'udvalg'
    """
    fieldnames = fieldnames + [
        "Fornavn",
        "Efternavn",
        "Brugernavn",
        "Post",
        "Leder",
        "Tillidrepræsentant",
        "E-mail",
        "Telefon",
    ]
    rows = []
    for node in PreOrderIter(nodes["root"]):
        path_dict = mh._create_path_dict(fieldnames, node, org_types)
        if not path_dict:
            continue
        employees = mh.read_organisation_people(
            node.name, person_type="association", split_name=True
        )
        for uuid, employee in employees.items():
            row = {}
            address = mh.read_user_address(uuid, username=True)
            mh.read_user_manager_status(uuid)
            if mh.read_user_manager_status(uuid):
                row["Leder"] = "Ja"
            if "Tillidrepræsentant" in mh.read_user_roller(uuid):
                row[" Tillidrepræsentant"] = "Ja"
            row.update(path_dict)  # Path
            row.update(address)  # Brugernavn
            row.update(employee)  # Everything else
            rows.append(row)
    mh._write_csv(fieldnames, rows, filename)


def write_multiple_managers_from_graphql_payload(
    mh: MoraHelper, gql_session: SyncClientSession, filename
) -> None:
    """
    This function will call upon GraphQL queries to retrieve necessary manager
    details for each wanted Organisation Unit and all of its children recursively.
    All details will then be written as a CSV formatted report on manager(s), with any
    desired details. Fields may be modified and added, as seen fit. As of now
    the following fields will be written:
    "root", "org", "sub org" (will keep adding sub orgs till the height of the root),
    "Ansvar", "Navn, "Telefon" and "E-mail.
    Multiple managers are supported.

    If no managers are found, the fields will be written with empty values.

    :args:
    MoraHelpers to find Organisation Units recursively.
    A GraphQL session to perform queries.
    """

    def get_email_from_address_object(employee: dict) -> str | None:
        """
        Function for extracting the e-mail address of the manager.

        :args:
        A Manager response with an "addresses" object.

        :returns:
        The first e-mail address of the manager, if the filter applied is successful.
        Will return None, if no email was found.

        :example:
        "'benth@kolding.dk'"
        """
        if employee.get("objects")[0]["employee"] is not None:
            filtered_email_address_object = list(
                filter(
                    lambda address_type: address_type["address_type"]["scope"]
                    == "EMAIL",
                    one(one(employee["objects"])["employee"])["addresses"],
                )
            )
            if filtered_email_address_object:
                return first(filtered_email_address_object)["name"]
        else:
            return None  # Empty E-mails.

    def get_phone_from_address_object(employee: dict) -> str | None:
        """
        Function for extracting the phone number of the manager.

        :args:
        A Manager response with an "addresses" object.

        :returns:
        The first phone number of the manager, if the filter applied is successful.
        Will return None, if no phone number was found.

        :example:
        "'67338448'"
        """
        if employee.get("objects")[0]["employee"] is not None:
            filtered_phone_address_object = list(
                filter(
                    lambda address_type: address_type["address_type"]["scope"]
                    == "PHONE",
                    one(one(employee["objects"])["employee"])["addresses"],
                )
            )
            if filtered_phone_address_object:
                return first(filtered_phone_address_object)["name"]
        else:
            return None  # Empty phones.

    def get_name_from_manager_object(employee: dict) -> str | None:
        """
        Function for extracting the name of the manager.

        :args:
        A Manager object.

        :returns:
        The name of the manager.

        :example:
        "'Bent Lindstrøm Hansen'"
        """
        if employee.get("objects") and employee["objects"][0].get("employee") and employee["objects"][0]["employee"][
            0].get("name"):
            return employee["objects"][0]["employee"][0]["name"]
        else:
            return None

    def get_responsibilities_from_manager_object(
        manager_responsibility: dict,
    ) -> str | None:
        """
        This function will extract the primary responsibility of a manager.
        According to old logic from MoraHelpers, the main responsibility is
        "Personale: ansættelse/afskedigelse" - if this was not found, the
        primary responsibility would default to the last element in the list.

        :args:
        A Manager response with a "responsibilities" object.

        :returns:
        The name of the primary responsibility, if the filter applied is successful.
        Will return the last element in the "responsibilities" list, if filter did
        not find primary responsibility.
        Will return None, if manager has no responsibilities.

        :example:
        "'Personale: ansættelse/afskedigelse'" if successful.

        "'Personale: MUS-kompetence'" if filter was not successful:
        """
        responsibilities = one(manager_responsibility["objects"])["responsibilities"]
        filtered_responsibility_object = list(
            filter(
                lambda primary_responsibility: primary_responsibility["full_name"]
                == "Personale: ansættelse/afskedigelse",  # According to MoraHelpers.
                responsibilities,
            )
        )
        if filtered_responsibility_object:
            return first(filtered_responsibility_object)["full_name"]
        elif responsibilities:  # Default in MoraHelpers.
            return responsibilities[-1]["full_name"]
        else:  # In case of no responsibilities found.
            return None

    fieldnames = mh._create_fieldnames(nodes)
    fieldnames += ["Ansvar", "Navn", "Telefon", "E-mail"]
    rows = []
    for node in PreOrderIter(
        nodes["root"]
    ):  # Finding Organisation Unit nodes recursively.
        list_of_manager_object_data = get_managers_for_export(gql_session, node.name)
        if list_of_manager_object_data:  # If managers are found.
            for manager in list_of_manager_object_data:
                row = {}
                # Finding all the OUs children and writing a "sub org" field for each child node.
                root_org_and_all_its_children = mh._create_path_dict(fieldnames, node)
                row.update(root_org_and_all_its_children)
                row["Navn"] = get_name_from_manager_object(
                    manager
                )  # Name of the manager.
                row["Ansvar"] = get_responsibilities_from_manager_object(
                    manager
                )  # Responsibility.
                row["E-mail"] = get_email_from_address_object(
                    manager
                ) # Retrieving e-mail.
                row["Telefon"] = get_phone_from_address_object(
                    manager
                )  # Retrieving phone number.
                rows.append(row)
        if (
            not list_of_manager_object_data
        ):  # If not managers are found, write empty values to CSV.
            row = {}
            root_org_and_all_its_children = mh._create_path_dict(fieldnames, node)
            row.update(root_org_and_all_its_children)  # Path
            rows.append(row)

    mh._write_csv(fieldnames, rows, filename)


if __name__ == "__main__":
    settings = get_common_query_export_settings()
    settings.start_logging_based_on_settings()
    gql_client = setup_gql_client(settings=settings)
    threaded_speedup = False
    t = time.time()

    mh = MoraHelper(hostname=MORA_BASE, export_ansi=False)

    org = mh.read_organisation()
    roots = mh.read_top_units(org)
    print(roots)
    for root in roots:
        if root["name"] == "Ballerup Kommune":
            ballerup = root["uuid"]
        if root["name"] == "9B":
            sd = root["uuid"]
        if root["name"] == "H-MED Hoved-MED":
            udvalg = root["uuid"]

    if threaded_speedup:
        cq.pre_cache_users(mh)
        print(f"Build cache: {time.time() - t}")

    nodes = mh.read_ou_tree(ballerup)
    print(f"Read nodes: {time.time() - t}s")

    with gql_client as session:
        print("Initiating a GraphQL session.")
        print("Retrieving queries to write from.")

        write_multiple_managers_from_graphql_payload(
            mh, session, "exporters/yolololo.csv"  # settings.alle_leder_funktioner_file_path
        )
        print("Successfully wrote all necessary manager details to csv.")

    print(f"Alle ledere: {time.time() - t}s")

    cq.export_all_employees(mh, nodes, settings.alle_bk_stilling_email_file_path)
    print("AlleBK-stilling-email: {}s".format(time.time() - t))

    cq.export_orgs(mh, nodes, settings.ballerup_org_inc_medarbejdere_file_path)
    print("Ballerup org incl medarbejdere: {}s".format(time.time() - t))

    cq.export_adm_org(
        mh, nodes, settings.adm_org_incl_start_og_stopdata_og_enhedstyper_file_path
    )
    print("Adm-org-incl-start-stop: {}s".format(time.time() - t))

    cq.export_all_teams(mh, nodes, settings.teams_tilknyttede_file_path)
    print("Teams: {}s".format(time.time() - t))

    nodes = mh.read_ou_tree(sd)
    cq.export_orgs(
        mh, nodes, settings.sd_loen_org_med_pnr_file_path, include_employees=False
    )
    print("SD-løn: {}".format(time.time() - t))

    nodes = mh.read_ou_tree(udvalg)
    fieldnames = ["Hoved-MED", "Center-MED", "Lokal-MED", "AMR-Gruppe"]
    org_types = ["AMR"]
    export_udvalg(
        mh,
        nodes,
        settings.amr_udvalgsmedlemmer_i_hieraki_file_path,
        fieldnames,
        org_types,
    )
    print("AMR: {}".format(time.time() - t))

    fieldnames = ["Hoved-MED", "Center-MED", "Lokal-MED", "AMR-Gruppe"]
    org_types = ["H-MED", "C-MED", "L-MED"]
    export_udvalg(
        mh,
        nodes,
        settings.med_udvalgsmedlemmer_i_hieraki_file_path,
        fieldnames,
        org_types,
    )
    print("MED: {}".format(time.time() - t))

    print("Export completed")
