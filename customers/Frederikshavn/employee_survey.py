#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from datetime import date

import pandas as pd
from anytree import PreOrderIter
from fastramqpi.ra_utils.load_settings import load_settings
from fastramqpi.raclients.upload import file_uploader
from gql import gql
from more_itertools import first
from more_itertools import one

from reports.graphql import get_mo_client
from reports.shared_reports import CustomerReports

# --------------------------------------------------------------------------------------
# Code
# --------------------------------------------------------------------------------------


def age_from_cpr(cpr_no: str | None) -> str:
    if cpr_no is None:
        return "-"
    year = int(cpr_no[4:6])
    code_msd = int(cpr_no[6])
    century: int = 0
    if code_msd < 4:
        century = 1900
    elif code_msd in {4, 9}:
        if 0 <= year <= 36:
            century = 2000
        else:
            century = 1900
    elif 5 <= code_msd <= 8:
        if 0 <= year <= 57:
            century = 2000
        else:
            century = 1800
    return str(date.today().year - (century + year))


def gender_guess_from_cpr(cpr_no: str | None) -> str:
    """Return "Kvinde", "Mand" or "-".

    Male or female CPR is guessed by the last digit, "-" is returned
    when there is no CPR available.
    """
    if cpr_no is None:
        return "-"

    if int(cpr_no) % 2:
        return "Mand"

    return "Kvinde"


class Survey(CustomerReports):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def org_unit_overview(self) -> pd.DataFrame:
        rows = []
        for node in PreOrderIter(self.nodes["root"]):
            ou = self.read_ou(node.name)
            parent_id = ou["parent"]["user_key"] if ou["parent"] else ""
            manager = self.read_ou_manager(node.name, inherit=True)
            address = dict()
            if manager:
                address = self.read_user_address(manager["uuid"])
            rows.append(
                {
                    "OrgID": ou.get("user_key"),
                    "ParentID": parent_id,
                    "Enhedsnavn": ou["name"],
                    "Leder for enhed": manager.get("Navn") or "",
                    "Email på leder": address.get("E-mail") or "",
                }
            )
        return pd.DataFrame(rows)

    def employees(self) -> pd.DataFrame:
        query = """
          query OrgunitEmployees($uuid: UUID!) {
            org_units(filter: { uuids: [$uuid] }) {
              objects {
                current {
                  name
                  engagements {
                    user_key
                    person {
                      name
                      cpr_number
                      addresses(filter: { address_type: { scope: "EMAIL" } }) {
                        name
                      }
                    }
                  }
                  managers {
                        user_key
                      }
                }
              }
            }
          }
        """

        rows = []
        for node in PreOrderIter(self.nodes["root"]):
            res = self.graphql_client.execute(
                gql(query), variable_values={"uuid": node.name}
            )
            org_unit = one(res["org_units"]["objects"])["current"]
            for eng in org_unit["engagements"]:
                user_key = eng["user_key"]

                person = one(eng["person"])
                addresses = first(person["addresses"], default=None)
                email = addresses["name"] if addresses else ""
                # The manager-role has the same user-key as engagements.
                # It is imported this way from opus to be able to match managers to engagements
                emp_type = (
                    "Leder"
                    if user_key in {m["user_key"] for m in org_unit["managers"]}
                    else "Medarbejder"
                )
                rows.append(
                    {
                        "Medarbejdernr": user_key,
                        "Respondentnavn": person["name"],
                        "OrgID": org_unit["name"],
                        "Enhedsnavn": org_unit["name"],
                        "E-mail": email,
                        "Type": emp_type,
                        "Alder": age_from_cpr(person["cpr_number"]),
                        "Køn": gender_guess_from_cpr(person["cpr_number"]),
                    }
                )

        return pd.DataFrame(rows)


def main() -> None:
    # Settings
    settings = load_settings()
    host = settings["mora.base"]
    org = settings["reports.org_name"]

    # Survey
    graphql_client = get_mo_client(
        mo_base_url=settings["mora.base"],
        client_id=settings["crontab.CLIENT_ID"],
        client_secret=settings["crontab.CLIENT_SECRET"],
        auth_server=settings["crontab.AUTH_SERVER"],
        gql_version=25,
    )

    survey = Survey(host, org, graphql_client)

    with file_uploader(settings, "Datasæt til trivselsundersøgelse.xlsx") as filename:
        with pd.ExcelWriter(filename) as writer:
            survey.org_unit_overview().to_excel(
                writer, sheet_name="Organisation", index=False
            )
            survey.employees().to_excel(writer, sheet_name="Medarbejdere", index=False)


if __name__ == "__main__":
    main()
