from functools import lru_cache

from ra_utils.job_settings import JobSettings


class EmployeePhoneBookSettings(JobSettings):
    # Common settings for Frederikshavn
    report_dir_path: str = "/opt/docker/os2mo/queries"

    # FTP settings for Frederikshavn:
    ftp_url: str | None
    ftp_port: int | None
    ftp_user: str | None
    ftp_pass: str | None
    ftp_folder: str | None

    # Settings for Employee Phonebook:
    # TODO remove these once the script is ready to run, and set them in Salt.
    sql_cell_phone_number_field: str | None = (
        "AD-Mobil"  # Desired cell phone type - "AD-Mobil".
    )
    sql_phone_number_field_list: list | None = [
        "AD-Telefonnummer",
        "Telefon",
    ]  # Desired phone type -
    # "AD-Telefonnummer" and "Telefon".
    sql_visibility_scope_field: str | None = (
        "SECRET"  # Exclude visibility scope of - "SECRET".
    )
    sql_visibility_title_field: str | None = (
        "Hemmelig"  # Exclude visibility scope of - "Hemmelig".
    )
    sql_excluded_organisation_units_user_key: str | None = (
        "1018136"  # Exclude certain organisation units.
    )
    sql_excluded_organisation_units_uuid: str | None = (
        "f11963f6-2df5-9642-f1e3-0983dad332f4"  # Exclude certain
    )
    # organisation units by uuid.


class ImproperlyConfigured(Exception):
    pass


@lru_cache()
def get_employee_phone_book_settings(*args, **kwargs) -> EmployeePhoneBookSettings:
    return EmployeePhoneBookSettings(*args, **kwargs)
