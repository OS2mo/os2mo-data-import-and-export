import io

import pandas as pd
from more_itertools import prepend
from sqlalchemy import and_

from customers.Frederikshavn.config import EmployeePhoneBookSettings
from customers.Frederikshavn.ftp_connnector import SFTPFileSet, upload_csv_to_ftps_server
from exporters.sql_export.sql_table_defs import Adresse, Bruger, Engagement, Enhed
from reports.query_actualstate import run_report, run_report_as_csv, set_of_org_units


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
            Adresse.adressetype_titel == settings.sql_cell_phone_number_field,
            and_(
                Adresse.synlighed_scope != settings.sql_visibility_scope_field,
                Adresse.synlighed_titel != settings.sql_visibility_title_field,
            ),
        )
        .subquery()
    )

    Phonenr = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel.in_(settings.sql_phone_number_field_list),
            Adresse.synlighed_scope != settings.sql_visibility_scope_field,
            Adresse.synlighed_titel != settings.sql_visibility_title_field,
        )
        .subquery()
    )

    Afdelinger = (
        session.query(Enhed.navn)
        .filter(Enhed.bvn != settings.sql_excluded_organisation_units_user_key)
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
            Engagement.enhed_uuid != settings.sql_excluded_organisation_units_uuid,
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
    ftp = SFTPFileSet()
    settings = EmployeePhoneBookSettings()
    settings.start_logging_based_on_settings()
    file_path = settings.report_dir_path

    print("Initiating report.")
    run_report(
        list_employees_for_phonebook,
        "Medarbejdertelefonbog",
        "Frederikshavn Kommune",
        file_path + "/Medarbejdertelefonbog.xlsx",
    )
    print("Report successfully done!")

    print("Initiating CSV report.")
    run_report_as_csv(
        list_employees_for_phonebook,
        "Frederikshavn Kommune",
        file_path + "/Medarbejdertelefonbog.csv",
    )
    upload_csv_to_ftps_server(
        server=settings.ftp_url, username=settings.ftp_user,
        password=settings.ftp_pass, csv_data=file_path + "/Medarbejdertelefonbog.csv",
        file_name="MedarbejderTelefonbog", target_folder=settings.ftp_folder)
    # with open(file_path + "/Medarbejdertelefonbog.csv", "r") as f:
    #     s = io.StringIO(f.read())
    #     ftp.write_file("Medarbejder Telefonbog", s, folder=settings.ftp_folder)
    print("CSV report successfully done!")
