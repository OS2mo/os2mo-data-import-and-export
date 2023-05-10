import pandas as pd
from more_itertools import prepend
from sqlalchemy import and_

from customers.Frederikshavn.config import EmployeePhoneBookSettings
from exporters.sql_export.sql_table_defs import Adresse
from exporters.sql_export.sql_table_defs import Bruger
from exporters.sql_export.sql_table_defs import Engagement
from exporters.sql_export.sql_table_defs import Enhed
from reports.query_actualstate import run_report
from reports.query_actualstate import run_report_as_csv
from reports.query_actualstate import set_of_org_units


def list_employees_for_phonebook(session, org_name: str) -> list:
    """
    Lists all employees in an organisation unit with relevant fields.

    Args:
        session: A SQLAlchemy session to make queries on.
        org_name: Name of the organisation unit to retrieve data from.

    Returns:
        A list of tuples with titles as first element and data on
        employees in subsequent tuples.

    Example:
        [
            ("Navn", "Mobil", "Telefon", "Enhed", "Stilling"),
            ("Fornavn Efternavn", 0123456789, "12345678",
                "Enhedsnavn", "Stillingsbetegnelse")
        ]
    """
    alle_enheder = set_of_org_units(session, org_name)

    Cellphonenr = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "AD-Mobil",
            and_(
                Adresse.synlighed_scope != "SECRET",
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )

    Phonenr = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel.in_(
                ["AD-Telefonnummer", "Telefon"]
            ),  # settings.sql_phone_number_field
            Adresse.synlighed_scope != "SECRET",  # settings.sql_visibility_scope_field
            Adresse.synlighed_titel != "Hemmelig",
        )
        .subquery()
    )

    Afdelinger = (
        session.query(Enhed.navn)
        .filter(
            Enhed.bvn
            != "1018136"  # settings.sql_excluded_organisation_units_user_key  # 1018136
        )
        .subquery()
    )

    query = (
        session.query(
            Bruger.fornavn + " " + Bruger.efternavn,
            Cellphonenr.c.værdi,
            Phonenr.c.værdi,
            Afdelinger.c.navn,
            Engagement.stillingsbetegnelse_titel,
        )
        .filter(
            Enhed.uuid == Engagement.enhed_uuid,
            Engagement.enhed_uuid.in_(alle_enheder),
            Engagement.enhed_uuid != "f11963f6-2df5-9642-f1e3-0983dad332f4",  # settings
            # .sql_excluded_organisation_units_uuid,
            Engagement.bruger_uuid == Bruger.uuid,
        )
        .join(Cellphonenr, Cellphonenr.c.bruger_uuid == Bruger.uuid, isouter=True)
        .join(Phonenr, Phonenr.c.bruger_uuid == Bruger.uuid, isouter=True)
        .join(Afdelinger, Afdelinger.c.navn == Enhed.navn, isouter=True)
        .order_by(Bruger.efternavn)
    )
    data = query.all()
    data_df = pd.DataFrame(
        data,
        columns=[
            "Navn",
            "Mobil nr.",
            "Telefon nr.",
            "Afdeling",
            "Stillingsbetegnelse",
        ],
    )
    print(data_df.columns)

    # Return data as a list of tuples with columns as the first element
    parsed_data = list(prepend(data_df.columns, data_df.to_records(index=False)))
    return parsed_data


if __name__ == "__main__":
    settings = EmployeePhoneBookSettings()
    settings.start_logging_based_on_settings()
    file_path = settings.report_dir_path

    print("Initiating report.")
    run_report(
        list_employees_for_phonebook,
        "Medarbejdertelefonbog",
        "Frederikshavn Kommune",
        file_path + "/TestMedarbejdertelefonbog.xlsx",
    )
    print("Report successfully done!")

    print("Initiating CSV report.")
    run_report_as_csv(
        list_employees_for_phonebook,
        "Frederikshavn Kommune",
        file_path + "/TestMedarbejdertelefonbog.csv",
    )
    print("CSV report successfully done!")
