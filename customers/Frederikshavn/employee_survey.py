#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from datetime import date
from pathlib import Path

import pandas as pd
from anytree import PreOrderIter
from ra_utils.load_settings import load_settings

from reports.shared_reports import CustomerReports

# --------------------------------------------------------------------------------------
# Code
# --------------------------------------------------------------------------------------


def age_from_cpr(cpr_no: str) -> int:
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
    return date.today().year - (century + year)


class Survey(CustomerReports):
    def __init__(self, hostname: str, org_name: str):
        super().__init__(hostname, org_name)

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
        rows = []
        for node in PreOrderIter(self.nodes["root"]):
            employees = self.read_organisation_people(node.name)
            for uuid, employee in employees.items():
                address = self.read_user_address(uuid, cpr=True)
                engagements = self.read_user_engagements(uuid)
                gender = "Kvinde"
                if int(address["CPR-Nummer"]) % 2:
                    gender = "Mand"
                user_id = ", ".join([eng.get("user_key") for eng in engagements])
                org_id = ", ".join([eng["org_unit"]["user_key"] for eng in engagements])
                org_name = ", ".join([eng["org_unit"]["name"] for eng in engagements])
                emp_type = (
                    "Leder"
                    if self._mo_lookup(uuid, "e/{}/details/")["manager"]
                    else "Medarbejder"
                )
                name = employee["Fornavn"] + " " + employee["Efternavn"]
                rows.append(
                    {
                        "Medarbejdernr": user_id,
                        "Respondentnavn": name,
                        "OrgID": org_id,
                        "Enhedsnavn": org_name,
                        "E-mail": address.get("E-mail") or "",
                        "Type": emp_type,
                        "Alder": age_from_cpr(address["CPR-Nummer"]),
                        "Køn": gender,
                    }
                )

        return pd.DataFrame(rows)


def main() -> None:
    # Settings
    settings = load_settings()
    host = settings["mora.base"]
    org = settings["reports.org_name"]
    outdir = Path(settings["mora.folder.query_export"])

    # Survey
    survey = Survey(host, org)

    with pd.ExcelWriter(outdir / "Datasæt til trivselsundersøgelse.xlsx") as writer:
        survey.org_unit_overview().to_excel(
            writer, sheet_name="Organisation", index=False
        )
        survey.employees().to_excel(writer, sheet_name="Medarbejdere", index=False)


if __name__ == "__main__":
    main()
