#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from csv import QUOTE_ALL
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import pandas as pd
from anytree import PreOrderIter
from os2mo_helpers.mora_helpers import MoraHelper


# --------------------------------------------------------------------------------------
# CustomerReports class
# --------------------------------------------------------------------------------------


class CustomerReports(MoraHelper):
    """Collection of shared customer reports. Subclasses MoraHelper.

    Attributes
    ----------
    nodes: Dict[str, Any]
        Dictionary containing the organisation tree.
    """

    def __init__(self, hostname: str, org_name: str):
        """Initialise customer reports with hostname and
        organisation name.

        Parameters
        ----------
        hostname : str
            MoRa host
        org_name : str
            The organisation name, e.g. Testkommune

        Raises
        ------
        ValueError
            If the organisation name (and thus UUID) is not found
            in the top units.
        """
        super().__init__(hostname=hostname)
        self.nodes: Dict[str, Any] = dict()

        # This sucks, sorry
        org = super().read_organisation()
        top_units = super().read_top_units(org)
        for unit in top_units:
            if org_name in unit["name"]:
                self.nodes = super().read_ou_tree(unit["uuid"])
                break
        else:
            raise ValueError(
                f"Organisation unit {org_name} not found in organisation units"
            )

    def get_org_cols(self) -> List[str]:
        """Get suborganisation columns dynamically.

        Returns
        -------
        List[str]
            List of column names.
        """

        cols = ["root", "org", "sub org"]
        for i in range(2, self.nodes["root"].height):
            cols += [str(i) + "xsub org"]
        return cols

    def employees(self) -> pd.DataFrame:
        """Generate a report listing employees in the organisation.

        Returns
        -------
        pd.DataFrame
            pandas DataFrame containing employee information.
        """

        rows = []
        cols = [
            "CPR-Nummer",
            "Ansættelse gyldig fra",
            "Ansættelse gyldig til",
            "Fornavn",
            "Efternavn",
            "Person UUID",
            "Brugernavn",
            "Org-enhed",
            "Org-enhed UUID",
            "E-mail",
            "Telefon",
            "Stillingsbetegnelse",
            "Engagement UUID",
        ]

        for node in PreOrderIter(self.nodes["root"]):
            employees = self.read_organisation_people(node.name)
            for uuid, employee in employees.items():
                address = self.read_user_address(uuid, username=True, cpr=True)
                rows.append({**address, **employee})

        return pd.DataFrame(rows, columns=cols)

    def managers(self) -> pd.DataFrame:
        """Generate a report listing managers in the organisation.

        Returns
        -------
        pd.DataFrame
            pandas DataFrame containing manager information.
        """
        rows = []
        cols = self.get_org_cols()
        cols.extend(["Ansvar", "Navn", "Telefon", "E-mail"])

        for node in PreOrderIter(self.nodes["root"]):
            manager = self.read_ou_manager(node.name)
            if manager:
                path_dict = self._create_path_dict(cols, node)
                address = self.read_user_address(manager["uuid"])
                rows.append({**path_dict, **manager, **address})

        return pd.DataFrame(rows, columns=cols)

    def organisation_overview(self) -> pd.DataFrame:
        """Generate a report listing the organisation structure including P-numbers.

        Returns
        -------
        pd.DataFrame
            pandas DataFrame containing organisation structure information.
        """
        rows = []
        cols = self.get_org_cols()
        cols.extend(["Adresse", "P-nummer"])

        for node in PreOrderIter(self.nodes["root"]):
            path_dict = self._create_path_dict(cols, node)
            org_address = self.read_ou_address(node.name)
            pnumber_dict: Dict[str, Any] = self.read_ou_address(
                node.name, scope="PNUMBER"
            )
            row = {**org_address, **path_dict, "P-nummer": pnumber_dict.get("value")}
            rows.append(row)

        return pd.DataFrame(rows, columns=cols)

    def organisation_employees(self) -> pd.DataFrame:
        """Returns an overview of employees within the organisation structure.

        Returns
        -------
        pd.DataFrame
            pandas Dataframe containing employee and organisation structure information.
        """
        rows = []
        cols = self.get_org_cols()
        cols.extend(["Navn", "Brugernavn", "Telefon", "E-mail", "Adresse"])

        for node in PreOrderIter(self.nodes["root"]):
            path_dict = self._create_path_dict(cols, node)
            org_address = self.read_ou_address(node.name)
            row = {**org_address, **path_dict}
            employees = self.read_organisation_people(node.name, split_name=False)
            for uuid, employee in employees.items():
                address = self.read_user_address(uuid, username=True)
                row.update({**address, **employee})
                rows.append(row)

        return pd.DataFrame(rows, columns=cols)

    def organisation_units(self) -> pd.DataFrame:
        """Generate a report listing organisation units within the organisation,
        including unit types and validity.

        Returns
        -------
        pd.DataFrame
            pandas DataFrame containing organisation unit information.
        """
        rows = []
        cols = [
            "uuid",
            "Navn",
            "Enhedtype UUID",
            "Gyldig fra",
            "Gyldig til",
            "Enhedstype Titel",
        ]
        for node in PreOrderIter(self.nodes["root"]):
            ou = self.read_ou(node.name)
            fra = ou["validity"]["from"]
            til = ou["validity"]["to"]
            over_uuid = ou["parent"]["uuid"] if ou["parent"] else ""
            row = {
                "uuid": ou["uuid"],
                "Overordnet ID": over_uuid,
                "Navn": ou["name"],
                "Enhedtype UUID": ou["org_unit_type"]["uuid"],
                "Gyldig fra": fra,
                "Gyldig til": til,
                "Enhedstype Titel": ou["org_unit_type"]["name"],
            }
            rows.append(row)

        return pd.DataFrame(rows, columns=cols)


# --------------------------------------------------------------------------------------
# Report utils
# --------------------------------------------------------------------------------------


def report_to_csv(df: pd.DataFrame, csv_out: Path) -> None:
    """Export a pandas DataFrame based report to CSV with
    specific settings.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to export to CSV
    csv_out : Path
        File to output CSV to.
    """
    df.to_csv(csv_out, sep=";", index=False, quoting=QUOTE_ALL)
