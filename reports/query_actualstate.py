# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Program to fetch data from an actualstate sqlitedatabase, written for creating
#  excel-reports with XLSXExporte.py
# See customers/Frederikshavn/Frederikshavn_reports.py for an example
from typing import Dict

import numpy as np
import pandas as pd
import xlsxwriter
from gql import gql
from more_itertools import prepend
from pydantic import BaseSettings
from raclients.graph.client import GraphQLClient
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import (
    Adresse,
    Bruger,
    Engagement,
    Enhed,
    Tilknytning,
)
from reports.XLSXExporter import XLSXExporter


def expand_org_path(df: pd.DataFrame, path_col: str) -> pd.DataFrame:
    # Create new dataframe with organisational path as columns
    org_paths = df[path_col].str.split("\\", expand=True)
    new_cols = [f"Enhed {column+1}" for column in org_paths.columns]
    org_paths.columns = new_cols

    # Remove the path column and join org_paths instead
    df = df.drop(columns=path_col)
    return df.join(org_paths)


def set_of_org_units(session, org_name: str) -> set:
    """Find all uuids of org_units under the organisation  :code:`org_name`."""

    hoved_enhed = session.query(Enhed.uuid).filter(Enhed.navn == org_name).one()[0]

    # Find all children of the unit and collect in a set
    def find_children(enheder):
        """Return a set of children under :code:`enheder`."""
        under_enheder = (
            session.query(Enhed.uuid)
            .filter(Enhed.forældreenhed_uuid.in_(enheder))
            .all()
        )
        # query returns a list of tuples like [(uuid2,),(uuid2,)],
        # so extract the first item in each.
        return set(enheder[0] for enheder in under_enheder)

    under_enheder = find_children(set([hoved_enhed]))
    alle_enheder = under_enheder
    # Update the set with any new units
    while under_enheder:
        under_enheder = find_children(under_enheder)
        alle_enheder.update(under_enheder)

    return alle_enheder


class Settings(BaseSettings):
    mora_base: str = "http://localhost:5000"
    client_id: str = "dipex"
    client_secret: str
    auth_realm: str = "mo"
    auth_server: str = "http://localhost:5000/auth"


def map_dynamic_class(result: list) -> Dict[str, str]:
    import jmespath

    dynamic_classes = jmespath.compile("objects[0].dynamic_class.name")
    dynamic_class_parents = jmespath.compile("objects[0].dynamic_class.parent.name")
    return {
        e["uuid"]: f"{dynamic_class_parents.search(e)} / {dynamic_classes.search(e)}"
        if dynamic_class_parents.search(e)
        else dynamic_classes.search(e)
        for e in result
    }


def fetch_dynamic_class(association_uuids: list[str]) -> Dict[str, str]:
    """Reads dynamic class for the associations with uuids from the given list

    Returns a map of association_uuids to dynamic class name
    including parent class if there is one.

    """

    settings = Settings()
    query = gql(
        """
            query employeeDynamicClasses($uuids: [UUID!]) {
                associations(uuids: $uuids) {
                    objects {
                        dynamic_class {
                        name
                        parent {
                            name
                        }
                    }
                    }
                }
            }
            """
    )

    with GraphQLClient(
        url=f"{settings.mora_base}/graphql",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_realm=settings.auth_realm,
        auth_server=settings.auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:

        r = session.execute(
            query,
            variable_values={
                # UUIDs are not JSON serializable, so they are converted to strings
                "uuids": association_uuids,
            },
        )

    return map_dynamic_class(r)


def merge_dynamic_classes(
    data_df: pd.DataFrame, association_dynamic_classes: Dict[str, str]
) -> pd.DataFrame:
    association_df = pd.DataFrame(
        association_dynamic_classes.items(),
        columns=["Tilknytningsuuid", "Hovedorganisation / Faglig organisation"],
    )
    data_df = data_df.merge(association_df, on="Tilknytningsuuid", how="left")
    return data_df.replace({np.nan: None})


def list_MED_members(session, org_names: dict) -> list:
    """Lists all "tilknyntninger" to an organisation.

    Returns a list of tuples with titles as first element
    and data on members in subsequent tuples. Example:
    [
        ("Navn", "Email", "Tilknytningstype", "Enhed"),
        ("Fornavn Efternavn", "email@example.com", "Formand", "Enhed")
    ]
    """
    alle_enheder = set_of_org_units(session, org_names["løn"])
    alle_MED_enheder = set_of_org_units(session, org_names["MED"])
    Emails = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "AD-Email",
            or_(
                Adresse.synlighed_titel.is_(None),
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )
    Phonenr = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "AD-Telefonnummer",
            or_(
                Adresse.synlighed_titel.is_(None),
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )
    eng_unit = (
        session.query(
            Enhed.navn, Enhed.organisatorisk_sti, Engagement.bruger_uuid
        ).filter(
            Enhed.uuid == Engagement.enhed_uuid,
            Engagement.enhed_uuid.in_(alle_enheder),
            Engagement.bruger_uuid == Bruger.uuid,
        )
    ).subquery()

    query = (
        session.query(
            Tilknytning.uuid,
            Bruger.fornavn + " " + Bruger.efternavn,
            Emails.c.værdi,
            Phonenr.c.værdi,
            Tilknytning.tilknytningstype_titel,
            Enhed.navn,
            eng_unit.c.navn,
            eng_unit.c.organisatorisk_sti,
        )
        .filter(
            Enhed.uuid == Tilknytning.enhed_uuid,
            Tilknytning.enhed_uuid.in_(alle_MED_enheder),
            Tilknytning.bruger_uuid == Bruger.uuid,
        )
        .join(Emails, Emails.c.bruger_uuid == Bruger.uuid, isouter=True)
        .join(Phonenr, Phonenr.c.bruger_uuid == Bruger.uuid, isouter=True)
        .join(eng_unit, eng_unit.c.bruger_uuid == Bruger.uuid)
        .order_by(Bruger.efternavn)
    )
    data = query.all()
    data_df = pd.DataFrame(
        data,
        columns=[
            "Tilknytningsuuid",
            "Navn",
            "Email",
            "Telefonnummer",
            "Tilknytningstype",
            "Tilknytningsenhed",
            "Ansættelsesenhed",
            "Sti",
        ],
    )
    data_df = expand_org_path(data_df, "Sti")
    # Add dynamic class info:
    association_dynamic_class = fetch_dynamic_class(list(data_df.Tilknytningsuuid))

    data_df = merge_dynamic_classes(data_df, association_dynamic_class)
    # Return data as a list of tuples with columns as the first element
    data_df = data_df.drop(columns="Tilknytningsuuid")
    parsed_data = list(prepend(data_df.columns, data_df.to_records(index=False)))

    return parsed_data


def list_org_units(session, org_name: str) -> list:
    query = session.query(Enhed.bvn, Enhed.organisatorisk_sti)
    data = query.all()
    data_df = pd.DataFrame(data, columns=["Enhedsnr", "Sti"])
    data_df = expand_org_path(data_df, "Sti")
    # Return data as a list of tuples with columns as the first element
    parsed_data = list(prepend(data_df.columns, data_df.to_records(index=False)))
    return parsed_data


def list_employees(session, org_name: str) -> list:
    """Lists all employees in organisation.

    Returns a list of tuples with titles as first element
    and data on employees in subsequent tuples. Example:
    [
        (Navn", "CPR", "Email", "Telefon", "Enhed", "Stilling"),
        ("Fornavn Efternavn", 0123456789,  "email@example.com", "12345678",
            "Enhedsnavn", "Stillingsbetegnelse")
    ]
    """
    alle_enheder = set_of_org_units(session, org_name)

    Emails = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "AD-Email",
            or_(
                Adresse.synlighed_titel.is_(None),
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )
    Phonenr = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "AD-Telefonnummer",
            or_(
                Adresse.synlighed_titel.is_(None),
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )
    query = (
        session.query(
            Bruger.uuid,
            Bruger.fornavn + " " + Bruger.efternavn,
            Bruger.cpr,
            Emails.c.værdi,
            Phonenr.c.værdi,
            Enhed.navn,
            Enhed.organisatorisk_sti,
            Engagement.stillingsbetegnelse_titel,
        )
        .filter(
            Enhed.uuid == Engagement.enhed_uuid,
            Engagement.enhed_uuid.in_(alle_enheder),
            Engagement.bruger_uuid == Bruger.uuid,
        )
        .join(Emails, Emails.c.bruger_uuid == Bruger.uuid, isouter=True)
        .join(Phonenr, Phonenr.c.bruger_uuid == Bruger.uuid, isouter=True)
        .order_by(Bruger.efternavn)
    )
    data = query.all()
    data_df = pd.DataFrame(
        data,
        columns=[
            "UUID",
            "Navn",
            "CPR",
            "AD-Email",
            "AD-Telefonnummer",
            "Enhed",
            "Sti",
            "Stilling",
        ],
    )
    data_df = expand_org_path(data_df, "Sti")
    # Return data as a list of tuples with columns as the first element
    parsed_data = list(prepend(data_df.columns, data_df.to_records(index=False)))
    return parsed_data


def run_report(reporttype, sheetname: str, org_name: str, xlsx_file: str):

    # Make a sqlalchemy session - Name of database is read from settings
    session = sessionmaker(bind=get_engine(), autoflush=False)()

    # Make the query
    data = reporttype(session, org_name)

    # write data as excel file
    workbook = xlsxwriter.Workbook(xlsx_file)
    excel = XLSXExporter(xlsx_file)
    excel.add_sheet(workbook, sheetname, data)
    workbook.close()
