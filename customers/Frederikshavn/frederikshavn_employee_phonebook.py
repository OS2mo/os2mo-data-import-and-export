import logging
import shutil

import pandas as pd
from fastramqpi.raclients.upload import file_uploader
from more_itertools import prepend
from sqlalchemy import and_
from sqlalchemy import or_

from customers.Frederikshavn.config import EmployeePhoneBookSettings
from exporters.sql_export.sql_table_defs import WAdresse as Adresse
from exporters.sql_export.sql_table_defs import WBruger as Bruger
from exporters.sql_export.sql_table_defs import WEngagement as Engagement
from exporters.sql_export.sql_table_defs import WEnhed as Enhed
from reports.query_actualstate import run_report
from reports.query_actualstate import run_report_as_csv
from reports.query_actualstate import set_of_org_units

logger = logging.getLogger(__name__)


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
            Adresse.adressetype_titel.in_(settings.sql_phone_number_field_list),  # type: ignore
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
            Engagement.uuid,
            Bruger.fornavn,
            Bruger.efternavn,
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
            or_(Cellphonenr.c.værdi != "", Phonenr.c.værdi != ""),
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
            "Username",
            "Fornavn",
            "Efternavn",
            "Mobil nr.",
            "Telefon nr.",
            "Afdeling",
            "Stillingsbetegnelse",
        ],
    )

    # Return data as a list of tuples with columns as the first element
    parsed_data = list(prepend(data_df.columns, data_df.to_records(index=False)))
    return parsed_data


if __name__ == "__main__":
    logger.info("Finding settings")
    settings = EmployeePhoneBookSettings()  # type: ignore
    settings.start_logging_based_on_settings()
    logger.info("Settings in place. Initiating report.")

    with file_uploader(settings, "Medarbejdertelefonbog.xlsx") as filename:
        run_report(
            list_employees_for_phonebook,
            "Medarbejdertelefonbog",
            "Frederikshavn Kommune",
            filename,
        )
    logger.info("Ran employee xlsx format report successfully!")

    logger.info("Initiating CSV report.")
    with file_uploader(settings, "Medarbejdertelefonbog.csv") as filename:
        run_report_as_csv(
            list_employees_for_phonebook,
            "Frederikshavn Kommune",
            filename,
        )
        # Note that only the csv file is copied, not the xlsx file.
        shutil.copyfile(filename, "/tmp/Medarbejdertelefonbog.csv")
    logger.info("Ran employee CSV format report successfully!")
